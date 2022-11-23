use dozer_api::grpc::internal_grpc::pipeline_request::ApiEvent;
use dozer_api::grpc::internal_grpc::PipelineRequest;
use dozer_api::grpc::types_helper;
use dozer_cache::cache::{BatchedCacheMsg, Cache};
use dozer_cache::cache::{BatchedWriter, LmdbCache};
use dozer_core::dag::errors::{ExecutionError, SinkError};
use dozer_core::dag::node::{PortHandle, Sink, SinkFactory};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::crossbeam;
use dozer_types::crossbeam::channel::Sender;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::parking_lot::Mutex;
use dozer_types::types::FieldType;
use dozer_types::types::{
    IndexDefinition, Operation, Schema, SchemaIdentifier, SortDirection::Ascending,
};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<LmdbCache>,
    api_endpoint: ApiEndpoint,
    notifier: Option<Sender<PipelineRequest>>,
    record_cutoff: u32,
    timeout: u64,
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
        notifier: Option<crossbeam::channel::Sender<PipelineRequest>>,
        record_cutoff: u32,
        timeout: u64,
    ) -> Self {
        Self {
            input_ports,
            cache,
            api_endpoint,
            notifier,
            record_cutoff,
            timeout,
        }
    }
}

impl SinkFactory for CacheSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(&self) -> Box<dyn Sink> {
        Box::new(CacheSink::new(
            self.cache.clone(),
            self.api_endpoint.clone(),
            Mutex::new(HashMap::new()),
            self.notifier.clone(),
            self.record_cutoff,
            self.timeout,
        ))
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    counter: i32,
    before: Instant,
    input_schemas: Mutex<HashMap<PortHandle, Schema>>,
    api_endpoint: ApiEndpoint,
    pb: ProgressBar,
    notifier: Option<crossbeam::channel::Sender<PipelineRequest>>,
    batched_sender: Sender<BatchedCacheMsg>,
}

impl Sink for CacheSink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        // Insert schemas into cache

        for (k, schema) in input_schemas {
            let mut map = self.input_schemas.lock();

            // Append primary and secondary keys
            let schema = self.get_output_schema(schema)?;

            self.cache
                .insert_schema(&self.api_endpoint.name, &schema)
                .map_err(|e| {
                    ExecutionError::SinkError(SinkError::SchemaUpdateFailed(Box::new(e)))
                })?;

            map.insert(*k, schema.to_owned());

            if let Some(notifier) = &self.notifier {
                let schema = types_helper::map_schema(self.api_endpoint.name.to_owned(), &schema);
                let res = notifier
                    .try_send(PipelineRequest {
                        endpoint: self.api_endpoint.name.to_owned(),
                        api_event: Some(ApiEvent::Schema(schema)),
                    })
                    .map_err(|e| {
                        ExecutionError::SinkError(SinkError::SchemaNotificationFailed(Box::new(e)))
                    });

                match res {
                    Ok(_) => {
                        debug!("Schema notification succeeded.")
                    }
                    Err(_) => {
                        warn!("GRPC Schema notification failed")
                    }
                };
            }
        }
        Ok(())
    }

    fn init(&mut self, _tx: &mut dyn Environment) -> Result<(), ExecutionError> {
        info!("SINK: Initialising CacheSink");

        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        _seq: u64,
        op: Operation,
        _tx: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        self.counter += 1;
        if self.counter % 100 == 0 {
            self.pb.set_message(format!(
                "Count: {}, Elapsed time: {:.2?}",
                self.counter,
                self.before.elapsed(),
            ));
        }
        let schema = self
            .input_schemas
            .lock()
            .get(&from_port)
            .map_or(Err(ExecutionError::SchemaNotInitialized), Ok)?
            .to_owned();

        if let Some(notifier) = &self.notifier {
            let op = types_helper::map_operation(self.api_endpoint.name.to_owned(), &op);
            notifier
                .try_send(PipelineRequest {
                    endpoint: self.api_endpoint.name.to_owned(),
                    api_event: Some(ApiEvent::Op(op)),
                })
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }

        self.batched_sender
            .send(BatchedCacheMsg { op, schema })
            .unwrap();
        Ok(())
    }
}

impl CacheSink {
    pub fn new(
        cache: Arc<LmdbCache>,
        api_endpoint: ApiEndpoint,
        input_schemas: Mutex<HashMap<PortHandle, Schema>>,
        notifier: Option<crossbeam::channel::Sender<PipelineRequest>>,
        record_cutoff: u32,
        timeout: u64,
    ) -> Self {
        let (batched_sender, batched_receiver) =
            crossbeam::channel::bounded::<BatchedCacheMsg>(record_cutoff as usize);
        let cache_batch = cache.clone();

        thread::spawn(move || {
            let batched_writer = BatchedWriter {
                cache: cache_batch,
                receiver: batched_receiver,
                record_cutoff,
                timeout,
            };
            batched_writer.run().unwrap();
        });

        Self {
            cache,
            counter: 0,
            before: Instant::now(),
            input_schemas,
            api_endpoint,
            pb: get_progress(),
            notifier,
            batched_sender,
        }
    }
    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, ExecutionError> {
        let mut schema = schema.clone();

        // Get hash of schema
        let hash = self.get_schema_hash();

        let api_index = &self.api_endpoint.index;
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
        schema.secondary_indexes = schema
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
                | FieldType::Null => Some(IndexDefinition::SortedInverted(vec![(idx, Ascending)])),

                // Create full text indexes for text fields
                FieldType::Text => Some(IndexDefinition::FullText(idx)),

                // Skip creating indexes
                FieldType::Binary
                | FieldType::UIntArray
                | FieldType::IntArray
                | FieldType::FloatArray
                | FieldType::BooleanArray
                | FieldType::StringArray
                | FieldType::Bson => None,
            })
            .collect();
        Ok(schema)
    }

    fn get_schema_hash(&self) -> u64 {
        // Get hash of SQL
        let mut hasher = DefaultHasher::new();
        let bytes = self.api_endpoint.sql.as_bytes();
        hasher.write(bytes);

        hasher.finish()
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils;

    use dozer_cache::cache::{index, Cache};

    use dozer_core::dag::executor_local::DEFAULT_PORT_HANDLE;

    use dozer_core::dag::node::Sink;
    use dozer_core::storage::common::RenewableRwTransaction;
    use dozer_core::storage::lmdb_storage::LmdbEnvironmentManager;
    use dozer_core::storage::transactions::SharedTransaction;
    use dozer_types::parking_lot::RwLock;
    use dozer_types::types::{Field, Operation, Record, SchemaIdentifier};
    use std::sync::Arc;
    use std::{collections::HashMap, thread, time::Duration};
    use tempdir::TempDir;

    #[test]
    // This test cases covers updation of records when primary key changes because of value change in primary_key
    fn update_record_when_primary_changes() {
        let tmp_dir = TempDir::new("example").unwrap();
        let mut env = LmdbEnvironmentManager::create(tmp_dir.path(), "test").unwrap();
        let txn: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            Arc::new(RwLock::new(env.create_txn().unwrap()));

        let schema = test_utils::get_schema();

        let (cache, mut sink) = test_utils::init_sink(&schema);

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
                values: vec![Field::Int(1), Field::Null],
            },
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: updated_values.clone(),
            },
        };
        let mut input_schemas = HashMap::new();
        input_schemas.insert(DEFAULT_PORT_HANDLE, schema.clone());
        sink.update_schema(&input_schemas).unwrap();

        let mut t = SharedTransaction::new(&txn);
        sink.process(
            DEFAULT_PORT_HANDLE,
            0_u64,
            insert_operation,
            &mut t,
            &HashMap::new(),
        )
        .unwrap();

        thread::sleep(Duration::from_millis(100));
        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(initial_values, record.values);

        sink.process(
            DEFAULT_PORT_HANDLE,
            0_u64,
            update_operation,
            &mut t,
            &HashMap::new(),
        )
        .unwrap();

        thread::sleep(Duration::from_millis(100));
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
