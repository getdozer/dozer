use std::collections::HashSet;
use std::time::Duration;

use crate::grpc::types_helper;
use dozer_cache::dozer_log::reader::{LogReader, LogReaderOptions};
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
use metrics::{describe_counter, describe_histogram, histogram, increment_counter};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

pub async fn build_cache(
    cache: Box<dyn RwCache>,
    cancel: impl Future<Output = ()> + Unpin + Send + 'static,
    log_server_addr: String,
    log_reader_options: LogReaderOptions,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
    multi_pb: Option<MultiProgress>,
) -> Result<(), CacheError> {
    // Create log reader.
    let pos = cache.get_metadata()?.unwrap_or(0);
    debug!(
        "Starting log reader {} from position {pos}",
        log_reader_options.endpoint
    );
    let log_reader = LogReader::new(log_server_addr, log_reader_options, pos, multi_pb).await?;

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
    connections: &HashSet<String>,
    write_options: CacheWriteOptions,
) -> Result<Box<dyn RwCache>, CacheError> {
    match cache_manager.open_rw_cache(labels.clone(), write_options)? {
        Some(cache) => {
            debug_assert!(cache.get_schema() == &schema);
            Ok(cache)
        }
        None => {
            let cache = cache_manager.create_cache(
                labels,
                schema.0,
                schema.1,
                connections,
                write_options,
            )?;
            Ok(cache)
        }
    }
}

const READ_LOG_RETRY_INTERVAL: Duration = Duration::from_secs(1);

async fn read_log_task(
    mut cancel: impl Future<Output = ()> + Unpin + Send + 'static,
    mut log_reader: LogReader,
    sender: mpsc::Sender<(ExecutorOperation, u64)>,
) {
    loop {
        let next_op = std::pin::pin!(log_reader.next_op());
        match select(next_op, cancel).await {
            Either::Left((op, c)) => {
                let op = match op {
                    Ok(op) => op,
                    Err(e) => {
                        error!(
                            "Failed to read log: {e}, retrying after {READ_LOG_RETRY_INTERVAL:?}"
                        );
                        tokio::time::sleep(READ_LOG_RETRY_INTERVAL).await;
                        cancel = c;
                        continue;
                    }
                };

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

    const CACHE_OPERATION_COUNTER_NAME: &str = "cache_operation";
    describe_counter!(
        CACHE_OPERATION_COUNTER_NAME,
        "Number of message processed by cache builder"
    );

    const DATA_LATENCY_HISTOGRAM_NAME: &str = "data_latency";
    describe_histogram!(
        DATA_LATENCY_HISTOGRAM_NAME,
        "End-to-end data latency in seconds"
    );

    while let Some((op, pos)) = receiver.blocking_recv() {
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
                    let mut labels = cache.labels().clone();
                    labels.push("operation_type", "delete");
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);
                }
                Operation::Insert { mut new } => {
                    new.schema_id = schema.identifier;
                    let result = cache.insert(&new)?;
                    let mut labels = cache.labels().clone();
                    labels.push("operation_type", "insert");
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);

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
                    let mut labels = cache.labels().clone();
                    labels.push("operation_type", "update");
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);

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
            ExecutorOperation::Commit { epoch } => {
                cache.set_metadata(pos)?;
                cache.commit()?;
                if let Ok(duration) = epoch.decision_instant.elapsed() {
                    histogram!(
                        DATA_LATENCY_HISTOGRAM_NAME,
                        duration,
                        cache.labels().clone()
                    );
                }
            }
            ExecutorOperation::SnapshottingDone { connection_name } => {
                cache.set_metadata(pos)?;
                cache.set_connection_snapshotting_done(&connection_name)?;
                cache.commit()?;
            }
            ExecutorOperation::Terminate => {
                break;
            }
        }
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
