use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_api::grpc::internal_grpc::pipeline_response::ApiEvent;
use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_api::grpc::types_helper;
use dozer_cache::cache::expression::{QueryExpression, Skip};
use dozer_cache::cache::index::get_primary_key;
use dozer_cache::cache::{CacheManager, RwCache};
use dozer_core::epoch::Epoch;
use dozer_core::errors::{ExecutionError, SinkError};
use dozer_core::node::{PortHandle, Sink, SinkFactory};
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::SharedTransaction;
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_storage::lmdb_storage::LmdbExclusiveTransaction;
use dozer_types::crossbeam::channel::Sender;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::log::debug;
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use dozer_types::types::FieldType;
use dozer_types::types::{IndexDefinition, Operation, Schema, SchemaIdentifier};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub fn attach_progress(multi_pb: Option<MultiProgress>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    multi_pb.as_ref().map(|m| m.add(pb.clone()));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}
#[derive(Debug, Clone)]
pub struct CacheSinkSettings {
    flags: Option<Flags>,
    api_security: Option<ApiSecurity>,
}
impl CacheSinkSettings {
    pub fn new(flags: Option<Flags>, api_security: Option<ApiSecurity>) -> Self {
        Self {
            flags,
            api_security,
        }
    }
}
#[derive(Debug)]
pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<dyn RwCache>,
    api_endpoint: ApiEndpoint,
    notifier: Option<Sender<PipelineResponse>>,
    generated_path: PathBuf,
    multi_pb: MultiProgress,
    settings: CacheSinkSettings,
}

impl CacheSinkFactory {
    pub fn new(
        input_ports: Vec<PortHandle>,
        cache_manager: &dyn CacheManager,
        api_endpoint: ApiEndpoint,
        notifier: Option<Sender<PipelineResponse>>,
        generated_path: PathBuf,
        multi_pb: MultiProgress,
        settings: CacheSinkSettings,
    ) -> Result<Self, ExecutionError> {
        let alias = &api_endpoint.name;
        let cache = if let Some(cache) = cache_manager.open_rw_cache(alias).map_err(|e| {
            ExecutionError::SinkError(SinkError::CacheOpenFailed(alias.clone(), Box::new(e)))
        })? {
            cache
        } else {
            let cache = cache_manager.create_cache().map_err(|e| {
                ExecutionError::SinkError(SinkError::CacheCreateFailed(alias.clone(), Box::new(e)))
            })?;
            cache_manager
                .create_alias(cache.name(), alias)
                .map_err(|e| {
                    ExecutionError::SinkError(SinkError::CacheCreateAliasFailed {
                        alias: alias.clone(),
                        real_name: cache.name().to_string(),
                        source: Box::new(e),
                    })
                })?;
            cache
        };

        Ok(Self {
            input_ports,
            cache: cache.into(),
            api_endpoint,
            notifier,
            generated_path,
            multi_pb,
            settings,
        })
    }

    fn get_output_schema(
        &self,
        schema_id: u16,
        schema: &Schema,
    ) -> Result<(Schema, Vec<IndexDefinition>), ExecutionError> {
        let mut schema = schema.clone();

        // Generated Cache index based on api_index
        let configured_index = create_primary_indexes(
            &schema,
            &self.api_endpoint.index.to_owned().unwrap_or_default(),
        )?;
        // Generated schema in SQL
        let upstream_index = schema.primary_index.clone();

        let index = match (configured_index.is_empty(), upstream_index.is_empty()) {
            (true, true) => vec![],
            (true, false) => upstream_index,
            (false, true) => configured_index,
            (false, false) => {
                if !upstream_index.eq(&configured_index) {
                    return Err(ExecutionError::MismatchPrimaryKey {
                        endpoint_name: self.api_endpoint.name.clone(),
                        expected: get_field_names(&schema, &upstream_index),
                        actual: get_field_names(&schema, &configured_index),
                    });
                }
                configured_index
            }
        };

        schema.primary_index = index;

        schema.identifier = Some(SchemaIdentifier {
            id: schema_id as u32,
            version: 1,
        });

        // Automatically create secondary indexes
        let secondary_indexes = schema
            .fields
            .iter()
            .enumerate()
            .flat_map(|(idx, f)| match f.typ {
                // Create sorted inverted indexes for these fields
                FieldType::UInt
                | FieldType::Int
                | FieldType::Float
                | FieldType::Boolean
                | FieldType::Decimal
                | FieldType::Timestamp
                | FieldType::Date => vec![IndexDefinition::SortedInverted(vec![idx])],

                // Create sorted inverted and full text indexes for string fields.
                FieldType::String => vec![
                    IndexDefinition::SortedInverted(vec![idx]),
                    IndexDefinition::FullText(idx),
                ],

                // Create full text indexes for text fields
                // FieldType::Text => vec![IndexDefinition::FullText(idx)],
                FieldType::Text => vec![],

                // Skip creating indexes
                FieldType::Binary | FieldType::Bson => vec![],
            })
            .collect();
        Ok((schema, secondary_indexes))
    }
}

impl SinkFactory<SchemaSQLContext> for CacheSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn prepare(
        &self,
        input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        use std::println as stdinfo;
        // Insert schemas into cache
        debug!(
            "SinkFactory: Initialising CacheSinkFactory: {}",
            self.api_endpoint.name
        );
        for (schema_id, (schema, _ctx)) in input_schemas.into_iter() {
            stdinfo!(
                "SINK: Initializing output schema: {}",
                self.api_endpoint.name
            );
            let (pipeline_schema, secondary_indexes) =
                self.get_output_schema(schema_id, &schema)?;
            pipeline_schema.print().printstd();

            if self
                .cache
                .get_schema_and_indexes_by_name(&self.api_endpoint.name)
                .is_err()
            {
                self.cache
                    .insert_schema(
                        &self.api_endpoint.name,
                        &pipeline_schema,
                        &secondary_indexes,
                    )
                    .map_err(|e| {
                        ExecutionError::SinkError(SinkError::SchemaUpdateFailed(
                            self.api_endpoint.name.clone(),
                            Box::new(e),
                        ))
                    })?;
                debug!(
                    "SinkFactory: Inserted schema for {}",
                    self.api_endpoint.name
                );
            }

            ProtoGenerator::generate(
                &self.generated_path,
                &self.api_endpoint.name,
                schema,
                &self.settings.api_security,
                &self.settings.flags,
            )
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }

        Ok(())
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        let mut sink_schemas: HashMap<PortHandle, (Schema, Vec<IndexDefinition>)> = HashMap::new();
        // Insert schemas into cache
        for (k, schema) in input_schemas {
            let (schema, secondary_indexes) = self.get_output_schema(k, &schema)?;
            sink_schemas.insert(k, (schema, secondary_indexes));
        }
        Ok(Box::new(CacheSink::new(
            self.cache.clone(),
            self.api_endpoint.clone(),
            sink_schemas,
            self.notifier.clone(),
            Some(self.multi_pb.clone()),
        )))
    }
}

fn create_primary_indexes(
    schema: &Schema,
    api_index: &ApiIndex,
) -> Result<Vec<usize>, ExecutionError> {
    let mut primary_index = Vec::new();
    for name in api_index.primary_key.iter() {
        let idx = schema
            .fields
            .iter()
            .position(|fd| fd.name == name.clone())
            .map_or(Err(ExecutionError::FieldNotFound(name.to_owned())), |p| {
                Ok(p)
            })?;

        primary_index.push(idx);
    }
    Ok(primary_index)
}

fn get_field_names(schema: &Schema, indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|idx| schema.fields[*idx].name.to_owned())
        .collect()
}

#[derive(Debug)]
pub struct CacheSink {
    cache: Arc<dyn RwCache>,
    counter: usize,
    input_schemas: HashMap<PortHandle, (Schema, Vec<IndexDefinition>)>,
    api_endpoint: ApiEndpoint,
    pb: ProgressBar,
    notifier: Option<Sender<PipelineResponse>>,
}

impl Sink for CacheSink {
    fn commit(&mut self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        let endpoint_name = self.api_endpoint.name.clone();
        // Update Counter on commit
        self.pb.set_message(format!(
            "{}: Count: {}",
            self.api_endpoint.name.to_owned(),
            self.counter,
        ));
        self.cache.commit().map_err(|e| {
            ExecutionError::SinkError(SinkError::CacheCommitTransactionFailed(
                endpoint_name,
                Box::new(e),
            ))
        })?;
        Ok(())
    }

    fn init(&mut self, _txn: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        let query = QueryExpression::new(None, vec![], None, Skip::Skip(0));
        self.counter = self
            .cache
            .count(&self.api_endpoint.name, &query)
            .map_err(|e| {
                ExecutionError::SinkError(SinkError::CacheCountFailed(
                    self.api_endpoint.name.clone(),
                    Box::new(e),
                ))
            })?;

        debug!(
            "SINK: Initialising CacheSink: {} with count: {}",
            self.api_endpoint.name, self.counter
        );
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        mut op: Operation,
        _tx: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        self.counter += 1;

        let endpoint_name = self.api_endpoint.name.clone();

        let (schema, _) = self
            .input_schemas
            .get(&from_port)
            .ok_or(ExecutionError::SchemaNotInitialized)?;

        match &mut op {
            Operation::Delete { old } => {
                old.schema_id = schema.identifier;
                let key = get_primary_key(&schema.primary_index, &old.values);
                let version = self.cache.delete(&key).map_err(|e| {
                    ExecutionError::SinkError(SinkError::CacheDeleteFailed(
                        endpoint_name,
                        Box::new(e),
                    ))
                })?;
                old.version = Some(version);
            }
            Operation::Insert { new } => {
                new.schema_id = schema.identifier;
                self.cache.insert(new).map_err(|e| {
                    ExecutionError::SinkError(SinkError::CacheInsertFailed(
                        endpoint_name,
                        Box::new(e),
                    ))
                })?;
            }
            Operation::Update { old, new } => {
                old.schema_id = schema.identifier;
                new.schema_id = schema.identifier;
                let key = get_primary_key(&schema.primary_index, &old.values);
                let old_version = self.cache.update(&key, new).map_err(|e| {
                    ExecutionError::SinkError(SinkError::CacheUpdateFailed(
                        endpoint_name,
                        Box::new(e),
                    ))
                })?;
                old.version = Some(old_version);
            }
        }

        if let Some(notifier) = &self.notifier {
            let op = types_helper::map_operation(self.api_endpoint.name.clone(), &op);
            notifier
                .try_send(PipelineResponse {
                    endpoint: self.api_endpoint.name.clone(),
                    api_event: Some(ApiEvent::Op(op)),
                })
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }

        Ok(())
    }
}

impl CacheSink {
    pub fn new(
        cache: Arc<dyn RwCache>,
        api_endpoint: ApiEndpoint,
        input_schemas: HashMap<PortHandle, (Schema, Vec<IndexDefinition>)>,
        notifier: Option<Sender<PipelineResponse>>,
        multi_pb: Option<MultiProgress>,
    ) -> Self {
        let pb = attach_progress(multi_pb);
        Self {
            cache,
            counter: 0,
            input_schemas,
            api_endpoint,
            pb,
            notifier,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils;

    use dozer_cache::cache::index;
    use dozer_core::node::Sink;
    use dozer_core::storage::lmdb_storage::LmdbEnvironmentManager;
    use dozer_core::DEFAULT_PORT_HANDLE;

    use dozer_types::node::NodeHandle;
    use dozer_types::types::{Field, IndexDefinition, Operation, Record, SchemaIdentifier};
    use std::collections::HashMap;
    use tempdir::TempDir;

    #[test]
    // This test cases covers update of records when primary key changes because of value change in primary_key
    fn update_record_when_primary_changes() {
        let tmp_dir = TempDir::new("example").unwrap();
        let env =
            LmdbEnvironmentManager::create(tmp_dir.path(), "test", Default::default()).unwrap();
        let txn = env.create_txn().unwrap();

        let schema = test_utils::get_schema();
        let secondary_indexes: Vec<IndexDefinition> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![idx]))
            .collect();

        let (cache, mut sink) = test_utils::init_sink(&schema, secondary_indexes.clone());

        let mut input_schemas = HashMap::new();
        input_schemas.insert(DEFAULT_PORT_HANDLE, schema.clone());
        // sink.update_schema(&input_schemas).unwrap();

        // Initialing schemas
        cache
            .insert_schema("films", &schema, &secondary_indexes)
            .unwrap();

        let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];

        let updated_values = vec![
            Field::Int(2),
            Field::String("Film name updated".to_string()),
        ];

        let insert_operation = Operation::Insert {
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
        };

        let update_operation = Operation::Update {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: updated_values.clone(),
                version: None,
            },
        };

        sink.process(DEFAULT_PORT_HANDLE, insert_operation, &txn, &HashMap::new())
            .unwrap();
        sink.commit(
            &dozer_core::epoch::Epoch::from(
                0,
                NodeHandle::new(Some(DEFAULT_PORT_HANDLE), "".to_string()),
                0,
                0,
            ),
            &txn,
        )
        .unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        assert_eq!(initial_values, record.values);

        sink.process(DEFAULT_PORT_HANDLE, update_operation, &txn, &HashMap::new())
            .unwrap();
        let epoch1 = dozer_core::epoch::Epoch::from(
            0,
            NodeHandle::new(Some(DEFAULT_PORT_HANDLE), "".to_string()),
            0,
            1,
        );
        sink.commit(&epoch1, &txn).unwrap();

        // Primary key with old values
        let key = index::get_primary_key(&schema.primary_index, &initial_values);

        let record = cache.get(&key);

        assert!(record.is_err());

        // Primary key with updated values
        let key = index::get_primary_key(&schema.primary_index, &updated_values);
        let record = cache.get(&key).unwrap().record;

        assert_eq!(updated_values, record.values);
    }
}
