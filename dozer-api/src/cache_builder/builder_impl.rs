use std::sync::Arc;

use arc_swap::ArcSwap;
use dozer_cache::{
    cache::{CacheRecord, CacheWriteOptions, CommitState, RwCache, RwCacheManager, UpsertResult},
    dozer_log::{reader::OpAndPos, replication::LogOperation},
    errors::CacheError,
    CacheReader,
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

use crate::grpc::types_helper;

use super::endpoint_meta::EndpointMeta;

#[derive(Debug, PartialEq, Eq)]
struct CatchUpInfo {
    /// The log position that's being served. Need to catch up with it.
    serving_cache_next_log_position: u64,
    /// The endpoint meta that triggered the catch up.
    endpoint_meta: EndpointMeta,
}

pub struct CacheBuilderImpl {
    cache_manager: Arc<dyn RwCacheManager>,
    labels: Labels,
    building: Box<dyn RwCache>,
    next_log_position: u64,
    /// The cache that's being served.
    serving: Arc<ArcSwap<CacheReader>>,
    /// `Some`: The cache that's being built is behind the one being served,
    /// because the data source (log) has a different id and we have to rebuild the cache.
    ///
    /// `None`: The one that's being served is the one being built.
    catch_up_info: Option<CatchUpInfo>,
    progress_bar: ProgressBar,
}

impl CacheBuilderImpl {
    /// Checks if `serving` is consistent with `endpoint_meta`.
    /// If so, build the serving cache.
    /// If not, build a new cache and replace `serving` when the new cache catches up with `serving`.
    ///
    /// Returns the builder and the log position that the building cache starts with.
    pub fn new(
        cache_manager: Arc<dyn RwCacheManager>,
        serving: Arc<ArcSwap<CacheReader>>,
        endpoint_meta: EndpointMeta,
        labels: Labels,
        cache_write_options: CacheWriteOptions,
        progress_bar: ProgressBar,
    ) -> Result<(CacheBuilderImpl, u64), CacheError> {
        // Compare cache and log id.
        let this = if serving.load().cache_name() != endpoint_meta.cache_name() {
            let building = super::create_cache(
                &*cache_manager,
                endpoint_meta.clone(),
                labels.clone(),
                cache_write_options,
            )?;
            let serving_cache_next_log_position =
                next_log_position(serving.load().get_commit_state()?.as_ref());
            Self {
                cache_manager,
                labels,
                building,
                next_log_position: 0,
                serving,
                catch_up_info: Some(CatchUpInfo {
                    serving_cache_next_log_position,
                    endpoint_meta,
                }),
                progress_bar,
            }
        } else {
            let building =
                super::open_cache(&*cache_manager, endpoint_meta.clone(), labels.clone())?
                    .expect("cache should exist");
            let next_log_position = next_log_position(building.get_commit_state()?.as_ref());
            Self {
                cache_manager,
                labels,
                building,
                next_log_position,
                serving,
                catch_up_info: None,
                progress_bar,
            }
        };

        let next_log_position = this.next_log_position;
        this.progress_bar.set_position(next_log_position);

        Ok((this, next_log_position))
    }

    pub fn process_op(
        &mut self,
        op_and_pos: OpAndPos,
        operations_sender: Option<&(String, Sender<GrpcOperation>)>,
    ) -> Result<(), CacheError> {
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
                if let Some(catchup_info) = &self.catch_up_info {
                    if self.next_log_position >= catchup_info.serving_cache_next_log_position {
                        self.serving.store(Arc::new(
                            super::open_cache_reader(
                                &*self.cache_manager,
                                catchup_info.endpoint_meta.clone(),
                                self.labels.clone(),
                            )?
                            .expect("cache should exist"),
                        ));
                        self.catch_up_info = None;
                    }
                }
            }
            LogOperation::SnapshottingDone { connection_name } => {
                self.building
                    .set_connection_snapshotting_done(&connection_name)?;
            }
        }

        Ok(())
    }
}

fn next_log_position(commit_state: Option<&CommitState>) -> u64 {
    commit_state
        .map(|commit_state| commit_state.log_position + 1)
        .unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use dozer_cache::{
        cache::{LmdbRwCacheManager, RwCacheManager},
        dozer_log::schemas::EndpointSchema,
    };

    use super::*;

    const INITIAL_CACHE_NAME: &str = "initial_cache_name";
    const INITIAL_LOG_POSITION: u64 = 42;

    fn create_build_impl(log_id: String) -> (CacheBuilderImpl, u64) {
        let cache_manager: Arc<dyn RwCacheManager> =
            Arc::new(LmdbRwCacheManager::new(Default::default()).unwrap());
        let mut cache = super::super::create_cache(
            &*cache_manager,
            test_endpoint_meta(INITIAL_CACHE_NAME.to_string()),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        cache
            .commit(&CommitState {
                source_states: Default::default(),
                log_position: INITIAL_LOG_POSITION,
            })
            .unwrap();
        drop(cache);
        let serving = super::super::open_cache_reader(
            &*cache_manager,
            test_endpoint_meta(INITIAL_CACHE_NAME.to_string()),
            Default::default(),
        )
        .unwrap()
        .unwrap();
        let (builder, start) = CacheBuilderImpl::new(
            cache_manager,
            Arc::new(ArcSwap::from_pointee(serving)),
            test_endpoint_meta(log_id),
            Default::default(),
            Default::default(),
            ProgressBar::hidden(),
        )
        .unwrap();
        (builder, start)
    }

    fn test_endpoint_meta(log_id: String) -> EndpointMeta {
        EndpointMeta {
            name: Default::default(),
            log_id,
            schema: EndpointSchema {
                path: Default::default(),
                schema: Default::default(),
                secondary_indexes: Default::default(),
                enable_token: Default::default(),
                enable_on_event: Default::default(),
                connections: Default::default(),
            },
        }
    }

    #[test]
    fn test_builder_impl_new() {
        let (builder, start) = create_build_impl(INITIAL_CACHE_NAME.to_string());
        assert_eq!(start, INITIAL_LOG_POSITION + 1);
        assert_eq!(
            builder.building.name(),
            test_endpoint_meta(INITIAL_CACHE_NAME.to_string()).cache_name()
        );
        assert_eq!(builder.next_log_position, INITIAL_LOG_POSITION + 1);
        assert!(builder.catch_up_info.is_none());

        let new_log_id = "new_log_id";
        let (builder, start) = create_build_impl(new_log_id.to_string());
        assert_eq!(start, 0);
        assert_eq!(
            builder.building.name(),
            test_endpoint_meta(new_log_id.to_string()).cache_name()
        );
        assert_eq!(builder.next_log_position, 0);
        assert_eq!(
            builder.catch_up_info,
            Some(CatchUpInfo {
                serving_cache_next_log_position: INITIAL_LOG_POSITION + 1,
                endpoint_meta: test_endpoint_meta(new_log_id.to_string()),
            })
        );
    }

    #[test]
    fn test_builder_impl_process_op() {
        let new_log_id = "new_log_id";
        let (mut builder, _) = create_build_impl(new_log_id.to_string());

        for pos in 0..INITIAL_LOG_POSITION {
            builder
                .process_op(
                    OpAndPos {
                        op: LogOperation::Commit {
                            source_states: Default::default(),
                            decision_instant: SystemTime::now(),
                        },
                        pos,
                    },
                    None,
                )
                .unwrap();
            assert!(builder.catch_up_info.is_some());
            assert_eq!(
                builder.serving.load().cache_name(),
                test_endpoint_meta(INITIAL_CACHE_NAME.to_string()).cache_name()
            );
        }
        builder
            .process_op(
                OpAndPos {
                    op: LogOperation::Commit {
                        source_states: Default::default(),
                        decision_instant: SystemTime::now(),
                    },
                    pos: INITIAL_LOG_POSITION,
                },
                None,
            )
            .unwrap();
        assert!(builder.catch_up_info.is_none());
        assert_eq!(
            builder.serving.load().cache_name(),
            test_endpoint_meta(new_log_id.to_string()).cache_name()
        );
    }
}
