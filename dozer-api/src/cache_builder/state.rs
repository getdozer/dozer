use dozer_cache::{
    cache::{CacheRecord, CacheWriteOptions, CommitState, RwCache, RwCacheManager, UpsertResult},
    dozer_log::{reader::OpAndPos, replication::LogOperation},
    errors::CacheError,
};
use dozer_tracing::Labels;
use dozer_types::{
    grpc_types::types::Operation as GrpcOperation,
    indicatif::ProgressBar,
    log::error,
    types::{Field, Operation, Record, Schema},
};
use metrics::{describe_counter, describe_histogram, histogram, increment_counter};
use tokio::sync::broadcast::Sender;

use crate::{errors::ApiInitError, grpc::types_helper};

use super::endpoint_meta::EndpointMeta;

#[derive(Debug)]
struct CatchUpInfo {
    /// The log position that's being served. Need to catch up with it.
    serving_cache_next_log_position: u64,
    /// The endpoint meta that triggered the catch up.
    endpoint_meta: EndpointMeta,
}

#[derive(Debug)]
pub struct CacheBuilderState {
    building: Box<dyn RwCache>,
    next_log_position: u64,
    /// `Some`: The cache that's being built is behind the one being served,
    /// because the data source (log) has a different id and we have to rebuild the cache.
    ///
    /// `None`: The one that's being served is the one being built.
    catch_up_info: Option<CatchUpInfo>,
    progress_bar: ProgressBar,
}

impl CacheBuilderState {
    pub fn new(
        cache: Box<dyn RwCache>,
        progress_bar: ProgressBar,
    ) -> Result<CacheBuilderState, ApiInitError> {
        let next_log_position = next_log_position(&*cache)?;
        progress_bar.set_position(next_log_position);
        Ok(CacheBuilderState {
            building: cache,
            next_log_position,
            catch_up_info: None,
            progress_bar,
        })
    }

    pub fn update(
        &mut self,
        endpoint_meta: EndpointMeta,
        cache_manager: &dyn RwCacheManager,
        labels: Labels,
        cache_write_options: CacheWriteOptions,
    ) -> Result<(), ApiInitError> {
        if self.building.name() != endpoint_meta.log_id {
            let building = super::create_cache(
                cache_manager,
                endpoint_meta.clone(),
                labels,
                cache_write_options,
            )?;
            let next_log_position = 0;
            let serving_cache_next_log_position = self.serving_cache_next_log_position()?;
            let progress_bar = self.progress_bar.clone();
            progress_bar.set_position(next_log_position);

            *self = Self {
                building,
                next_log_position,
                catch_up_info: Some(CatchUpInfo {
                    serving_cache_next_log_position,
                    endpoint_meta,
                }),
                progress_bar,
            }
        }

        Ok(())
    }

    pub fn next_log_position(&self) -> u64 {
        self.next_log_position
    }

    /// Processes an op. Returns the `EndpointMeta` that the serving cache should be reopened with, if necessary.
    pub fn process_op(
        &mut self,
        op_and_pos: OpAndPos,
        operations_sender: Option<&(String, Sender<GrpcOperation>)>,
    ) -> Result<Option<EndpointMeta>, CacheError> {
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

        assert!(op_and_pos.pos == self.next_log_position);
        self.next_log_position += 1;

        self.progress_bar.set_position(self.next_log_position);
        match op_and_pos.op {
            LogOperation::Op { op } => match op {
                Operation::Delete { old } => {
                    if let Some(meta) = self.building.delete(&old)? {
                        if let Some((endpoint_name, operations_sender)) = operations_sender {
                            let operation = types_helper::map_delete_operation(
                                endpoint_name.clone(),
                                CacheRecord::new(meta.id, meta.version, old),
                            );
                            send_and_log_error(operations_sender, operation);
                        }
                    }
                    let mut labels = self.building.labels().clone();
                    labels.push(OPERATION_TYPE_LABEL, "delete");
                    labels.push(
                        SNAPSHOTTING_LABEL,
                        snapshotting_str(!self.building.is_snapshotting_done()?),
                    );
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);
                }
                Operation::Insert { new } => {
                    let result = self.building.insert(&new)?;
                    let mut labels = self.building.labels().clone();
                    labels.push(OPERATION_TYPE_LABEL, "insert");
                    labels.push(
                        SNAPSHOTTING_LABEL,
                        snapshotting_str(!self.building.is_snapshotting_done()?),
                    );
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);

                    if let Some((endpoint_name, operations_sender)) = operations_sender {
                        send_upsert_result(
                            endpoint_name,
                            operations_sender,
                            result,
                            &self.building.get_schema().0,
                            None,
                            new,
                        );
                    }
                }
                Operation::Update { old, new } => {
                    let upsert_result = self.building.update(&old, &new)?;
                    let mut labels = self.building.labels().clone();
                    labels.push(OPERATION_TYPE_LABEL, "update");
                    labels.push(
                        SNAPSHOTTING_LABEL,
                        snapshotting_str(!self.building.is_snapshotting_done()?),
                    );
                    increment_counter!(CACHE_OPERATION_COUNTER_NAME, labels);

                    if let Some((endpoint_name, operations_sender)) = operations_sender {
                        send_upsert_result(
                            endpoint_name,
                            operations_sender,
                            upsert_result,
                            &self.building.get_schema().0,
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
                self.building.commit(&CommitState {
                    source_states,
                    log_position: op_and_pos.pos,
                })?;
                if let Ok(duration) = decision_instant.elapsed() {
                    histogram!(
                        DATA_LATENCY_HISTOGRAM_NAME,
                        duration,
                        self.building.labels().clone()
                    );
                }

                // See if we have caught up with the cache being served.
                if let Some(catchup_info) = self.catch_up_info.take() {
                    if self.next_log_position >= catchup_info.serving_cache_next_log_position {
                        return Ok(Some(catchup_info.endpoint_meta));
                    } else {
                        self.catch_up_info = Some(catchup_info);
                    }
                }
            }
            LogOperation::SnapshottingDone { connection_name } => {
                self.building
                    .set_connection_snapshotting_done(&connection_name)?;
            }
        }

        Ok(None)
    }

    fn serving_cache_next_log_position(&self) -> Result<u64, ApiInitError> {
        if let Some(catchup_info) = &self.catch_up_info {
            Ok(catchup_info.serving_cache_next_log_position)
        } else {
            next_log_position(&*self.building)
        }
    }
}

fn next_log_position(cache: &dyn RwCache) -> Result<u64, ApiInitError> {
    Ok(cache
        .get_commit_state()
        .map_err(ApiInitError::GetCacheCommitState)?
        .map(|commit_state| commit_state.log_position + 1)
        .unwrap_or(0))
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
