use std::path::Path;

use crate::grpc::types_helper;
use dozer_cache::dozer_log::reader::LogReader;
use dozer_cache::{
    cache::{CacheRecord, CacheWriteOptions, RwCache, RwCacheManager, UpsertResult},
    errors::CacheError,
};
use dozer_types::epoch::ExecutorOperation;
use dozer_types::indicatif::MultiProgress;
use dozer_types::labels::Labels;
use dozer_types::log::debug;
use dozer_types::types::SchemaWithIndex;
use dozer_types::{
    grpc_types::types::Operation as GrpcOperation,
    log::error,
    types::{Field, Operation, Record, Schema},
};
use futures_util::stream::FuturesUnordered;
use futures_util::{
    future::{select, Either},
    Future,
};
use metrics::{describe_counter, increment_counter};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

pub async fn build_cache(
    cache: Box<dyn RwCache>,
    cancel: impl Future<Output = ()> + Unpin + Send + 'static,
    log_path: &Path,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
    multi_pb: Option<MultiProgress>,
) -> Result<(), CacheError> {
    // Create log reader.
    let pos = cache.get_metadata()?.unwrap_or(0);
    let reader_name = cache.labels().to_string();
    debug!("Starting log reader {reader_name} from position {pos}");
    let log_reader = LogReader::new(log_path, reader_name, pos, multi_pb).await?;

    // Spawn tasks
    let mut futures = FuturesUnordered::new();
    let (sender, receiver) = mpsc::channel(1);
    futures.push(tokio::spawn(async move {
        read_log_task(cancel, log_reader, sender).await;
        Ok(())
    }));
    futures.push({
        tokio::task::spawn_blocking(|| build_cache_task(cache, receiver, operations_sender))
    });

    while let Some(result) = futures.next().await {
        match result {
            Ok(Ok(())) => (),
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(CacheError::InternalThreadPanic(e)),
        }
    }

    Ok(())
}

pub fn open_or_create_cache(
    cache_manager: &dyn RwCacheManager,
    labels: Labels,
    schema: SchemaWithIndex,
    write_options: CacheWriteOptions,
) -> Result<Box<dyn RwCache>, CacheError> {
    match cache_manager.open_rw_cache(labels.clone(), write_options)? {
        Some(cache) => {
            debug_assert!(cache.get_schema() == &schema);
            Ok(cache)
        }
        None => {
            let cache = cache_manager.create_cache(labels, schema.0, schema.1, write_options)?;
            Ok(cache)
        }
    }
}

async fn read_log_task(
    mut cancel: impl Future<Output = ()> + Unpin + Send + 'static,
    mut log_reader: LogReader,
    sender: mpsc::Sender<(ExecutorOperation, u64)>,
) {
    loop {
        let next_op = std::pin::pin!(log_reader.next_op());
        match select(next_op, cancel).await {
            Either::Left((op, c)) => {
                cancel = c;
                if sender.send(op).await.is_err() {
                    debug!("Stop reading log because receiver is dropped");
                    break;
                }
            }
            Either::Right(_) => break,
        }
    }
}

fn build_cache_task(
    mut cache: Box<dyn RwCache>,
    mut receiver: mpsc::Receiver<(ExecutorOperation, u64)>,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
) -> Result<(), CacheError> {
    let schema = cache.get_schema().0.clone();

    const BUILD_CACHE_COUNTER_NAME: &str = "build_cache";
    describe_counter!(
        BUILD_CACHE_COUNTER_NAME,
        "Number of operations processed by cache builder"
    );

    while let Some((op, offset)) = receiver.blocking_recv() {
        match op {
            ExecutorOperation::Op { op } => match op {
                Operation::Delete { mut old } => {
                    old.schema_id = schema.identifier;
                    if let Some(meta) = cache.delete(&old)? {
                        if let Some((endpoint_name, operations_sender)) = operations_sender.as_ref()
                        {
                            let operation = types_helper::map_delete_operation(
                                endpoint_name.clone(),
                                CacheRecord::new(meta.id, meta.version, old),
                            );
                            send_and_log_error(operations_sender, operation);
                        }
                    }
                }
                Operation::Insert { mut new } => {
                    new.schema_id = schema.identifier;
                    let result = cache.insert(&new)?;

                    if let Some((endpoint_name, operations_sender)) = operations_sender.as_ref() {
                        send_upsert_result(
                            endpoint_name,
                            operations_sender,
                            result,
                            &schema,
                            None,
                            new,
                        );
                    }
                }
                Operation::Update { mut old, mut new } => {
                    old.schema_id = schema.identifier;
                    new.schema_id = schema.identifier;
                    let upsert_result = cache.update(&old, &new)?;

                    if let Some((endpoint_name, operations_sender)) = operations_sender.as_ref() {
                        send_upsert_result(
                            endpoint_name,
                            operations_sender,
                            upsert_result,
                            &schema,
                            Some(old),
                            new,
                        );
                    }
                }
            },
            ExecutorOperation::Commit { .. } => {
                cache.set_metadata(offset)?;
                cache.commit()?;
            }
            ExecutorOperation::SnapshottingDone {} => {
                cache.set_metadata(offset)?;
                cache.commit()?;
            }
            ExecutorOperation::Terminate => {
                break;
            }
        }

        increment_counter!(BUILD_CACHE_COUNTER_NAME, cache.labels().clone());
    }

    Ok(())
}

fn send_upsert_result(
    endpoint_name: &str,
    operations_sender: &Sender<GrpcOperation>,
    upsert_result: UpsertResult,
    schema: &Schema,
    old: Option<Record>,
    new: Record,
) {
    match upsert_result {
        UpsertResult::Inserted { meta } => {
            let op = types_helper::map_insert_operation(
                endpoint_name.to_string(),
                CacheRecord::new(meta.id, meta.version, new),
            );
            send_and_log_error(operations_sender, op);
        }
        UpsertResult::Updated { old_meta, new_meta } => {
            // If `old` is `None`, it means `Updated` comes from `Insert` operation.
            // In this case, we can't get the full old record, but the fields in the primary index must be the same with the new record.
            // So we create the old record with only the fields in the primary index, cloned from `new`.
            let old = old.unwrap_or_else(|| {
                let mut record = Record::new(new.schema_id, vec![Field::Null; new.values.len()]);
                for index in schema.primary_index.iter() {
                    record.values[*index] = new.values[*index].clone();
                }
                record
            });
            let op = types_helper::map_update_operation(
                endpoint_name.to_string(),
                CacheRecord::new(old_meta.id, old_meta.version, old),
                CacheRecord::new(new_meta.id, new_meta.version, new),
            );
            send_and_log_error(operations_sender, op);
        }
        UpsertResult::Ignored => {}
    }
}

fn send_and_log_error<T: Send + Sync + 'static>(sender: &Sender<T>, msg: T) {
    if let Err(e) = sender.send(msg) {
        error!("Failed to send broadcast message: {}", e);
    }
}
