use std::sync::Arc;

use arc_swap::ArcSwap;
use dozer_cache::{
    cache::{
        CacheRecord, CacheWriteOptions, CommitState, RoCache, RwCache, RwCacheManager, UpsertResult,
    },
    dozer_log::{reader::OpAndPos, replication::LogOperation},
    errors::CacheError,
    CacheReader,
};
use dozer_tracing::Labels;
use dozer_types::{
    grpc_types::types::Operation as GrpcOperation,
    indicatif::ProgressBar,
    log::error,
    types::{Field, Operation, Record, Schema, SchemaWithIndex},
};
use metrics::{describe_counter, describe_histogram, histogram, increment_counter};
use tokio::sync::broadcast::Sender;

use crate::{errors::ApiInitError, grpc::types_helper};

use super::endpoint_meta::EndpointMeta;

#[derive(Debug, PartialEq, Eq)]
struct CatchUpInfo {
    /// The log position that's being served. Need to catch up with it.
    serving_cache_next_log_position: u64,
    /// The endpoint meta that triggered the catch up.
    endpoint_meta: EndpointMeta,
}

#[derive(Debug)]
pub struct CacheBuilderState {
    cache_manager: Arc<dyn RwCacheManager>,
    labels: Labels,
    cache_write_options: CacheWriteOptions,
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

impl CacheBuilderState {
    pub fn new(
        cache_manager: Arc<dyn RwCacheManager>,
        labels: Labels,
        cache_write_options: CacheWriteOptions,
        endpoint_meta: EndpointMeta,
        progress_bar: ProgressBar,
    ) -> Result<CacheBuilderState, ApiInitError> {
        // Open or create cache.
        let cache = open_or_create_cache(
            &*cache_manager,
            endpoint_meta.clone(),
            labels.clone(),
            cache_write_options,
        )?;
        let serving = Arc::new(ArcSwap::from_pointee(
            open_cache_reader(&*cache_manager, endpoint_meta.clone(), labels.clone())
                .map_err(ApiInitError::OpenOrCreateCache)?,
        ));

        // Create state, assuming there's no need to catch up now.
        let next_log_position = next_log_position(&*cache)?;
        let mut this = CacheBuilderState {
            cache_manager,
            labels,
            cache_write_options,
            building: cache,
            next_log_position,
            serving,
            catch_up_info: None,
            progress_bar,
        };

        // Compare cache and log id.
        this.update(endpoint_meta)?;

        this.progress_bar.set_position(this.next_log_position);
        Ok(this)
    }

    pub fn cache_reader(&self) -> &Arc<ArcSwap<CacheReader>> {
        &self.serving
    }

    pub fn update(&mut self, endpoint_meta: EndpointMeta) -> Result<(), ApiInitError> {
        if self.building.name() != endpoint_meta.log_id {
            let building = create_cache(
                &*self.cache_manager,
                endpoint_meta.clone(),
                self.labels.clone(),
                self.cache_write_options,
            )?;
            let serving_cache_next_log_position = self.serving_cache_next_log_position()?;

            self.building = building;
            self.next_log_position = 0;
            self.catch_up_info = Some(CatchUpInfo {
                serving_cache_next_log_position,
                endpoint_meta,
            });
            self.progress_bar.set_position(self.next_log_position);
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
                        self.serving.store(Arc::new(open_cache_reader(
                            &*self.cache_manager,
                            catchup_info.endpoint_meta.clone(),
                            self.labels.clone(),
                        )?));
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

    fn serving_cache_next_log_position(&self) -> Result<u64, ApiInitError> {
        if let Some(catchup_info) = &self.catch_up_info {
            Ok(catchup_info.serving_cache_next_log_position)
        } else {
            next_log_position(&*self.building)
        }
    }
}

fn open_or_create_cache(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
    write_options: CacheWriteOptions,
) -> Result<Box<dyn RwCache>, ApiInitError> {
    let (alias, cache_labels) = endpoint_meta.cache_alias_and_labels(labels.clone());
    match cache_manager
        .open_rw_cache(alias.clone(), cache_labels, write_options)
        .map_err(ApiInitError::OpenOrCreateCache)?
    {
        Some(cache) => {
            check_cache_schema(
                cache.as_ro(),
                (
                    endpoint_meta.schema.schema,
                    endpoint_meta.schema.secondary_indexes,
                ),
            )
            .map_err(ApiInitError::OpenOrCreateCache)?;
            Ok(cache)
        }
        None => create_cache(cache_manager, endpoint_meta, labels, write_options),
    }
}

fn create_cache(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
    write_options: CacheWriteOptions,
) -> Result<Box<dyn RwCache>, ApiInitError> {
    let (alias, cache_labels) = endpoint_meta.cache_alias_and_labels(labels);
    let cache = cache_manager
        .create_cache(
            endpoint_meta.log_id.clone(),
            cache_labels,
            (
                endpoint_meta.schema.schema,
                endpoint_meta.schema.secondary_indexes,
            ),
            &endpoint_meta.schema.connections,
            write_options,
        )
        .map_err(ApiInitError::OpenOrCreateCache)?;
    cache_manager
        .create_alias(&endpoint_meta.log_id, &alias)
        .map_err(ApiInitError::OpenOrCreateCache)?;
    Ok(cache)
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
) -> Result<CacheReader, CacheError> {
    let (alias, cache_labels) = endpoint_meta.cache_alias_and_labels(labels);
    let cache = cache_manager
        .open_ro_cache(alias.clone(), cache_labels)?
        .expect("we've created this cache");
    check_cache_schema(
        &*cache,
        (
            endpoint_meta.schema.schema,
            endpoint_meta.schema.secondary_indexes,
        ),
    )?;
    Ok(CacheReader::new(cache))
}

fn check_cache_schema(cache: &dyn RoCache, given: SchemaWithIndex) -> Result<(), CacheError> {
    let stored = cache.get_schema();
    if &given != stored {
        return Err(CacheError::SchemaMismatch {
            name: cache.name().to_string(),
            given: Box::new(given),
            stored: Box::new(stored.clone()),
        });
    }
    Ok(())
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

    fn create_test_state() -> CacheBuilderState {
        let cache_manager: Arc<dyn RwCacheManager> =
            Arc::new(LmdbRwCacheManager::new(Default::default()).unwrap());
        let mut cache = open_or_create_cache(
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
        let state = CacheBuilderState::new(
            cache_manager,
            Default::default(),
            Default::default(),
            test_endpoint_meta(INITIAL_CACHE_NAME.to_string()),
            ProgressBar::hidden(),
        )
        .unwrap();
        assert_eq!(state.building.name(), INITIAL_CACHE_NAME);
        assert_eq!(state.next_log_position, INITIAL_LOG_POSITION + 1);
        assert!(state.catch_up_info.is_none());
        state
    }

    fn test_endpoint_meta(log_id: String) -> EndpointMeta {
        EndpointMeta {
            name: Default::default(),
            log_id,
            build_name: Default::default(),
            schema: EndpointSchema {
                path: Default::default(),
                schema: Default::default(),
                secondary_indexes: Default::default(),
                enable_token: Default::default(),
                enable_on_event: Default::default(),
                connections: Default::default(),
            },
            descriptor_bytes: Default::default(),
        }
    }

    #[test]
    fn test_state_update() {
        let mut state = create_test_state();
        state
            .update(test_endpoint_meta(INITIAL_CACHE_NAME.to_string()))
            .unwrap();
        assert_eq!(state.building.name(), INITIAL_CACHE_NAME);
        assert_eq!(state.next_log_position, INITIAL_LOG_POSITION + 1);
        assert!(state.catch_up_info.is_none());

        let new_log_id = "new_log_id";
        state
            .update(test_endpoint_meta(new_log_id.to_string()))
            .unwrap();
        assert_eq!(state.building.name(), new_log_id);
        assert_eq!(state.next_log_position, 0);
        assert_eq!(
            state.catch_up_info,
            Some(CatchUpInfo {
                serving_cache_next_log_position: INITIAL_LOG_POSITION + 1,
                endpoint_meta: test_endpoint_meta(new_log_id.to_string()),
            })
        );

        state
            .update(test_endpoint_meta(new_log_id.to_string()))
            .unwrap();
        assert_eq!(state.building.name(), new_log_id);
        assert_eq!(state.next_log_position, 0);
        assert_eq!(
            state.catch_up_info,
            Some(CatchUpInfo {
                serving_cache_next_log_position: INITIAL_LOG_POSITION + 1,
                endpoint_meta: test_endpoint_meta(new_log_id.to_string()),
            })
        );

        let new_log_id = "new_log_id_2";
        state
            .update(test_endpoint_meta(new_log_id.to_string()))
            .unwrap();
        assert_eq!(state.building.name(), new_log_id);
        assert_eq!(state.next_log_position, 0);
        assert_eq!(
            state.catch_up_info,
            Some(CatchUpInfo {
                serving_cache_next_log_position: INITIAL_LOG_POSITION + 1,
                endpoint_meta: test_endpoint_meta(new_log_id.to_string()),
            })
        );
    }

    #[test]
    fn test_state_process_op() {
        let mut state = create_test_state();
        let new_log_id = "new_log_id";
        state
            .update(test_endpoint_meta(new_log_id.to_string()))
            .unwrap();
        assert_eq!(state.building.name(), new_log_id);

        for pos in 0..INITIAL_LOG_POSITION {
            state
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
            assert!(state.catch_up_info.is_some());
            assert_eq!(state.serving.load().cache_name(), INITIAL_CACHE_NAME);
        }
        state
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
        assert!(state.catch_up_info.is_none());
        assert_eq!(state.serving.load().cache_name(), new_log_id);
    }
}
