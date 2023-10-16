use std::collections::HashSet;
use std::time::Duration;

use crate::grpc::types_helper;
use dozer_cache::dozer_log::reader::{LogReader, LogReaderBuilder, OpAndPos};
use dozer_cache::dozer_log::replication::LogOperation;
use dozer_cache::{
    cache::{CacheRecord, CacheWriteOptions, RwCache, RwCacheManager, UpsertResult},
    errors::CacheError,
};
use dozer_tracing::{Labels, LabelsAndProgress};
use dozer_types::indicatif::ProgressBar;
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
    log_reader_builder: LogReaderBuilder,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
    labels: LabelsAndProgress,
) -> Result<(), CacheError> {
    // Create log reader.
    let starting_pos = cache.get_log_position()?.map(|pos| pos + 1).unwrap_or(0);
    debug!(
        "Starting log reader {} from position {starting_pos}",
        log_reader_builder.options.endpoint
    );
    let pb = labels.create_progress_bar(format!("cache: {}", log_reader_builder.options.endpoint));
    pb.set_position(starting_pos);
    let log_reader = log_reader_builder.build(starting_pos);

    // Spawn tasks
    let mut futures = FuturesUnordered::new();
    let (sender, receiver) = mpsc::channel(1);
    futures.push(tokio::spawn(async move {
        read_log_task(cancel, log_reader, sender).await;
        Ok(())
    }));
    futures.push({
        tokio::task::spawn_blocking(|| build_cache_task(cache, receiver, operations_sender, pb))
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
    sender: mpsc::Sender<OpAndPos>,
) {
    loop {
        let next_op = std::pin::pin!(log_reader.read_one());
        match select(cancel, next_op).await {
            Either::Left(_) => break,
            Either::Right((op, c)) => {
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
        }
    }
}

fn build_cache_task(
    mut cache: Box<dyn RwCache>,
    mut receiver: mpsc::Receiver<OpAndPos>,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
    progress_bar: ProgressBar,
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

    const OPERATION_TYPE_LABEL: &str = "operation_type";
    const SNAPSHOTTING_LABEL: &str = "snapshotting";

    let mut snapshotting = !cache.is_snapshotting_done()?;

    while let Some(op_and_pos) = receiver.blocking_recv() {
        progress_bar.set_position(op_and_pos.pos + 1);
        match op_and_pos.op {
            LogOperation::Op { op } => match op {
                Operation::Delete { old } => {
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
                    labels.push(OPERATION_TYPE_LABEL, "delete");
                    labels.push(SNAPSHOTTING_LABEL, snapshotting_str(snapshotting));
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);
                }
                Operation::Insert { new } => {
                    let result = cache.insert(&new)?;
                    let mut labels = cache.labels().clone();
                    labels.push(OPERATION_TYPE_LABEL, "insert");
                    labels.push(SNAPSHOTTING_LABEL, snapshotting_str(snapshotting));
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
                Operation::Update { old, new } => {
                    let upsert_result = cache.update(&old, &new)?;
                    let mut labels = cache.labels().clone();
                    labels.push(OPERATION_TYPE_LABEL, "update");
                    labels.push(SNAPSHOTTING_LABEL, snapshotting_str(snapshotting));
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
            LogOperation::Commit {
                source_states,
                decision_instant,
            } => {
                cache.commit(&source_states, op_and_pos.pos)?;
                if let Ok(duration) = decision_instant.elapsed() {
                    histogram!(
                        DATA_LATENCY_HISTOGRAM_NAME,
                        duration,
                        cache.labels().clone()
                    );
                }
            }
            LogOperation::SnapshottingDone { connection_name } => {
                cache.set_connection_snapshotting_done(&connection_name)?;
                snapshotting = !cache.is_snapshotting_done()?;
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
                let mut record = Record::new(vec![Field::Null; new.values.len()]);
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

fn snapshotting_str(snapshotting: bool) -> &'static str {
    if snapshotting {
        "true"
    } else {
        "false"
    }
}
