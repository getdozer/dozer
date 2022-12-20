use dozer_api::grpc::internal_grpc::pipeline_response::ApiEvent;
use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_api::grpc::types_helper;
use dozer_cache::cache::index::get_primary_key;
use dozer_cache::cache::{
    lmdb_rs::{self, Transaction},
    Cache, LmdbCache,
};
use dozer_core::dag::errors::{ExecutionError, SinkError};
use dozer_core::dag::node::{NodeHandle, PortHandle, Sink, SinkFactory};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::crossbeam::channel::Sender;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::types::FieldType;
use dozer_types::types::{IndexDefinition, Operation, Schema, SchemaIdentifier};
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Instant;

pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<LmdbCache>,
    api_endpoint: ApiEndpoint,
    notifier: Option<Sender<PipelineResponse>>,
}

pub fn get_progress() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
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
impl CacheSinkFactory {
    pub fn new(
        input_ports: Vec<PortHandle>,
        cache: Arc<LmdbCache>,
        api_endpoint: ApiEndpoint,
        notifier: Option<Sender<PipelineResponse>>,
    ) -> Self {
        Self {
            input_ports,
            cache,
            api_endpoint,
            notifier,
        }
    }
    fn get_output_schema(
        &self,
        schema: &Schema,
    ) -> Result<(Schema, Vec<IndexDefinition>), ExecutionError> {
        let mut schema = schema.clone();

        // Get hash of schema
        let hash = self.get_schema_hash();

        let api_index = self.api_endpoint.index.to_owned().unwrap_or_default();
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
        schema.primary_index = primary_index;

        schema.identifier = Some(SchemaIdentifier {
            id: hash as u32,
            version: 1,
        });

        // Automatically create secondary indexes
        let secondary_indexes = schema
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, f)| match f.typ {
                // Create secondary indexes for these fields
                FieldType::UInt
                | FieldType::Int
                | FieldType::Float
                | FieldType::Boolean
                | FieldType::String
                | FieldType::Decimal
                | FieldType::Timestamp
                | FieldType::Date => Some(IndexDefinition::SortedInverted(vec![idx])),

                // Create full text indexes for text fields
                FieldType::Text => Some(IndexDefinition::FullText(idx)),

                // Skip creating indexes
                FieldType::Binary | FieldType::Bson => None,
            })
            .collect();
        Ok((schema, secondary_indexes))
    }

    fn get_schema_hash(&self) -> u64 {
        // Get hash of SQL
        let mut hasher = DefaultHasher::new();
        let bytes = self.api_endpoint.sql.as_bytes();
        hasher.write(bytes);

        hasher.finish()
    }
}

impl SinkFactory for CacheSinkFactory {
    fn set_input_schema(
        &self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        let mut sink_schemas: HashMap<PortHandle, (Schema, Vec<IndexDefinition>)> = HashMap::new();
        // Insert schemas into cache
        for (k, schema) in input_schemas {
            let (schema, secondary_indexes) = self.get_output_schema(&schema)?;
            sink_schemas.insert(k, (schema, secondary_indexes));
        }
        Ok(Box::new(CacheSink::new(
            self.cache.clone(),
            self.api_endpoint.clone(),
            sink_schemas,
            self.notifier.clone(),
        )))
    }
}

pub struct CacheSink {
    // It's not really 'static, the actual lifetime is the lifetime of `cache`. See comments in `process`.
    txn: Option<lmdb_rs::RwTransaction<'static>>,
    cache: Arc<LmdbCache>,
    counter: i32,
    before: Instant,
    input_schemas: HashMap<PortHandle, (Schema, Vec<IndexDefinition>)>,
    api_endpoint: ApiEndpoint,
    pb: ProgressBar,
    notifier: Option<Sender<PipelineResponse>>,
}

impl Sink for CacheSink {
    fn commit(
        &mut self,
        _source: &NodeHandle,
        _txid: u64,
        _seq_in_tx: u64,
        _tx: &mut dyn RwTransaction,
    ) -> Result<(), ExecutionError> {
        if let Some(txn) = self.txn.take() {
            txn.commit().map_err(|e| {
                ExecutionError::SinkError(SinkError::CacheCommitTransactionFailed(Box::new(e)))
            })?;
        }
        Ok(())
    }

    fn init(&mut self, _tx: &mut dyn Environment) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising CacheSink: {}", self.api_endpoint.name);

        // Insert schemas into cache
        for (_, (schema, secondary_indexes)) in self.input_schemas.iter() {
            self.cache
                .insert_schema(&self.api_endpoint.name, schema, secondary_indexes)
                .map_err(|e| {
                    ExecutionError::SinkError(SinkError::SchemaUpdateFailed(Box::new(e)))
                })?;
        }
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        _tx: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        self.counter += 1;
        if self.counter % 10 == 0 {
            self.pb.set_message(format!(
                "{}: Count: {}, Elapsed time: {:.2?}",
                self.api_endpoint.name.to_owned(),
                self.counter,
                self.before.elapsed(),
            ));
        }

        if self.txn.is_none() {
            let txn = self.cache.begin_rw_txn().map_err(|e| {
                ExecutionError::SinkError(SinkError::CacheBeginTransactionFailed(Box::new(e)))
            })?;
            // SAFETY:
            // 1. `std::mem::transmute` is only used to extend the lifetime of `txn` to `'static`.
            // 2. `RwTransaction` doesn't reference data in `LmdbCache`, the lifetime of it is only
            // to ensure that the returned `RwTransaction` does not outlive `LmdbCache`.
            // 3. `txn` in `CacheSink` is private, and we don't expose it to the outside, so the one owning
            // `txn` must own `CacheSink`.
            // 4. The declaration order in `CacheSink` ensures `txn` is dropped before `cache`.
            let txn = unsafe {
                std::mem::transmute::<lmdb_rs::RwTransaction<'_>, lmdb_rs::RwTransaction<'static>>(
                    txn,
                )
            };
            self.txn = Some(txn);
        }
        let txn = self.txn.as_mut().unwrap();

        let (schema, secondary_indexes) = self
            .input_schemas
            .get(&from_port)
            .ok_or(ExecutionError::SchemaNotInitialized)?;

        if let Some(notifier) = &self.notifier {
            let op = types_helper::map_operation(self.api_endpoint.name.to_owned(), &op);
            notifier
                .try_send(PipelineResponse {
                    endpoint: self.api_endpoint.name.to_owned(),
                    api_event: Some(ApiEvent::Op(op)),
                })
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }
        match op {
            Operation::Delete { old } => {
                let key = get_primary_key(&schema.primary_index, &old.values);
                self.cache
                    .delete_with_txn(txn, &key, &old, schema, secondary_indexes)
                    .map_err(|e| {
                        ExecutionError::SinkError(SinkError::CacheDeleteFailed(Box::new(e)))
                    })?;
            }
            Operation::Insert { new } => {
                // new.schema_id = schema.identifier.clone();
                self.cache
                    .insert_with_txn(txn, &new, schema, secondary_indexes)
                    .map_err(|e| {
                        ExecutionError::SinkError(SinkError::CacheInsertFailed(Box::new(e)))
                    })?;
            }
            Operation::Update { old, new } => {
                let key = get_primary_key(&schema.primary_index, &old.values);
                // new.schema_id = old.schema_id.clone();
                self.cache
                    .update_with_txn(txn, &key, &old, &new, schema, secondary_indexes)
                    .map_err(|e| {
                        ExecutionError::SinkError(SinkError::CacheUpdateFailed(Box::new(e)))
                    })?;
            }
        }

        Ok(())
    }
}

impl CacheSink {
    pub fn new(
        cache: Arc<LmdbCache>,
        api_endpoint: ApiEndpoint,
        input_schemas: HashMap<PortHandle, (Schema, Vec<IndexDefinition>)>,
        notifier: Option<Sender<PipelineResponse>>,
    ) -> Self {
        Self {
            txn: None,
            cache,
            counter: 0,
            before: Instant::now(),
            input_schemas,
            api_endpoint,
            pb: get_progress(),
            notifier,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils;
    use dozer_cache::cache::{index, Cache};

    use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
    use dozer_core::dag::node::{NodeHandle, Sink};
    use dozer_core::storage::common::RenewableRwTransaction;
    use dozer_core::storage::lmdb_storage::LmdbEnvironmentManager;
    use dozer_core::storage::transactions::SharedTransaction;
    use dozer_types::parking_lot::RwLock;
    use dozer_types::types::{Field, IndexDefinition, Operation, Record, SchemaIdentifier};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    // This test cases covers update of records when primary key changes because of value change in primary_key
    fn update_record_when_primary_changes() {
        let tmp_dir = TempDir::new("example").unwrap();
        let mut env = LmdbEnvironmentManager::create(tmp_dir.path(), "test").unwrap();
        let txn: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            Arc::new(RwLock::new(env.create_txn().unwrap()));

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
            },
        };

        let update_operation = Operation::Update {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
            },
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: updated_values.clone(),
            },
        };

        let mut t = SharedTransaction::new(&txn);
        sink.process(
            DEFAULT_PORT_HANDLE,
            insert_operation,
            &mut t,
            &HashMap::new(),
        )
        .unwrap();
        sink.commit(
            &NodeHandle::new(Some(DEFAULT_PORT_HANDLE), "".to_string()),
            0,
            0,
            &mut t,
        )
        .unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(initial_values, record.values);

        sink.process(
            DEFAULT_PORT_HANDLE,
            update_operation,
            &mut t,
            &HashMap::new(),
        )
        .unwrap();
        sink.commit(
            &NodeHandle::new(Some(DEFAULT_PORT_HANDLE), "".to_string()),
            0,
            1,
            &mut t,
        )
        .unwrap();

        // Primary key with old values
        let key = index::get_primary_key(&schema.primary_index, &initial_values);

        let record = cache.get(&key);

        assert!(record.is_err());

        // Primary key with updated values
        let key = index::get_primary_key(&schema.primary_index, &updated_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(updated_values, record.values);
    }
}
