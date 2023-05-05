use std::{path::Path, sync::Arc};

use crate::grpc::types_helper;
use dozer_cache::dozer_log::reader::LogReader;
use dozer_cache::{
    cache::{CacheRecord, CacheWriteOptions, RwCache, RwCacheManager, UpsertResult},
    errors::CacheError,
};
use dozer_types::epoch::ExecutorOperation;
use dozer_types::indicatif::MultiProgress;
use dozer_types::types::SchemaWithIndex;
use dozer_types::{
    grpc_types::types::Operation as GrpcOperation,
    log::error,
    types::{Field, Operation, Record, Schema},
};
use futures_util::{
    future::{select, Either},
    Future,
};
use tokio::{runtime::Runtime, sync::broadcast::Sender};

#[allow(clippy::too_many_arguments)]
pub async fn build_cache(
    cache_manager: &dyn RwCacheManager,
    alias: &str,
    schema: SchemaWithIndex,
    runtime: Arc<Runtime>,
    cancel: impl Future<Output = ()> + Unpin,
    log_path: &Path,
    write_options: CacheWriteOptions,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
    multi_pb: Option<MultiProgress>,
) -> Result<impl FnOnce() -> Result<(), CacheError>, CacheError> {
    let cache = open_or_create_cache(cache_manager, alias, schema.clone(), write_options)?;

    // Create log reader.
    let pos = cache.get_metadata()?.unwrap_or(0);
    let log_reader = LogReader::new(log_path, alias, pos, multi_pb).await?;

    // Create the task that builds cache.
    let task = move || {
        build_cache_task(
            cache,
            runtime,
            cancel,
            log_reader,
            schema.0,
            operations_sender,
        )
    };
    Ok(task)
}

fn open_or_create_cache(
    cache_manager: &dyn RwCacheManager,
    alias: &str,
    schema: SchemaWithIndex,
    write_options: CacheWriteOptions,
) -> Result<Box<dyn RwCache>, CacheError> {
    match cache_manager.open_rw_cache(alias, write_options)? {
        Some(cache) => {
            debug_assert!(cache.get_schema() == &schema);
            Ok(cache)
        }
        None => {
            let cache = cache_manager.create_cache(schema.0, schema.1, write_options)?;
            cache_manager.create_alias(cache.name(), alias)?;
            Ok(cache)
        }
    }
}

fn build_cache_task(
    mut cache: Box<dyn RwCache>,
    runtime: Arc<Runtime>,
    mut cancel: impl Future<Output = ()> + Unpin,
    mut log_reader: LogReader,
    schema: Schema,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
) -> Result<(), CacheError> {
    while let Some(op) = runtime.block_on(next_op_with_cancel(&mut log_reader, cancel)) {
        cancel = op.1;
        match op.0 {
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
                cache.set_metadata(log_reader.pos())?;
                cache.commit()?;
            }
            ExecutorOperation::SnapshottingDone {} => {
                cache.set_metadata(log_reader.pos())?;
                cache.commit()?;
            }
            ExecutorOperation::Terminate => {
                break;
            }
        }
    }

    Ok(())
}

async fn next_op_with_cancel<F: Future<Output = ()> + Unpin>(
    log_reader: &mut LogReader,
    cancel: F,
) -> Option<(ExecutorOperation, F)> {
    let next_op = Box::pin(log_reader.next_op());
    match select(next_op, cancel).await {
        Either::Left((op, cancel)) => Some((op, cancel)),
        Either::Right(_) => None,
    }
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
