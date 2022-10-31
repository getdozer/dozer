use dozer_cache::cache::LmdbCache;
use dozer_cache::cache::{index, Cache};
use dozer_types::core::node::PortHandle;
use dozer_types::core::node::{Sink, SinkFactory};
use dozer_types::core::state::{StateStore, StateStoreOptions};
use dozer_types::errors::execution::ExecutionError;
use dozer_types::errors::execution::ExecutionError::InternalStringError;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::types::{
    IndexDefinition, Operation, Schema, SchemaIdentifier, SortDirection::Ascending,
};
use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Instant;

pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<LmdbCache>,
    api_endpoint: ApiEndpoint,
    schema_change_notifier: crossbeam::channel::Sender<bool>,
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
        schema_change_notifier: crossbeam::channel::Sender<bool>,
    ) -> Self {
        Self {
            input_ports,
            cache,
            api_endpoint,
            schema_change_notifier,
        }
    }
}

impl SinkFactory for CacheSinkFactory {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        None
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(&self) -> Box<dyn Sink> {
        Box::new(CacheSink::new(
            self.cache.clone(),
            self.api_endpoint.clone(),
            HashMap::new(),
            HashMap::new(),
            Some(self.schema_change_notifier.clone()),
        ))
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    counter: i32,
    before: Instant,
    input_schemas: HashMap<PortHandle, Schema>,
    schema_map: HashMap<u64, bool>,
    api_endpoint: ApiEndpoint,
    pb: ProgressBar,
    schema_change_notifier: Option<crossbeam::channel::Sender<bool>>,
}

impl Sink for CacheSink {
    fn init(&mut self, _state_store: &mut dyn StateStore) -> Result<(), ExecutionError> {
        info!("SINK: Initialising CacheSink");
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        _seq: u64,
        op: Operation,
        _state: &mut dyn StateStore,
    ) -> Result<(), ExecutionError> {
        self.counter += 1;
        if self.counter % 100 == 0 {
            self.pb.set_message(format!(
                "Count: {}, Elapsed time: {:.2?}",
                self.counter,
                self.before.elapsed(),
            ));
        }

        let mut schema = self.get_output_schema(&self.input_schemas[&from_port])?;

        // Get hash of schema
        let mut hasher = DefaultHasher::new();
        let bytes = self.api_endpoint.sql.as_bytes();
        hasher.write(bytes);
        let hash = hasher.finish();

        schema.identifier = Some(SchemaIdentifier {
            id: hash as u32,
            version: 1,
        });

        // Automatically create secondary indexes
        schema.secondary_indexes = schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![(idx, Ascending)]))
            .collect();

        // Insert if schema not already inserted
        if let std::collections::hash_map::Entry::Vacant(e) = self.schema_map.entry(hash) {
            self.cache
                .insert_schema(&self.api_endpoint.name, &schema)
                .map_err(|e| InternalStringError(e.to_string()))?;
            e.insert(true);
            if let Some(notifier) = &self.schema_change_notifier {
                notifier
                    .try_send(true)
                    .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
            }
        }

        match op {
            Operation::Delete { old } => {
                let key = index::get_primary_key(&schema.primary_index, &old.values);
                self.cache
                    .delete(&key)
                    .map_err(|e| InternalStringError(e.to_string()))?;
            }
            Operation::Insert { new } => {
                let mut new = new;
                new.schema_id = schema.identifier;

                self.cache
                    .insert(&new)
                    .map_err(|e| InternalStringError(e.to_string()))?;
            }
            Operation::Update { old, new } => {
                let key = index::get_primary_key(&schema.primary_index, &old.values);
                let mut new = new;
                new.schema_id = schema.identifier.clone();
                if index::has_primary_key_changed(&schema.primary_index, &old.values, &new.values) {
                    self.cache
                        .update(&key, &new, &schema)
                        .map_err(|e| InternalStringError(e.to_string()))?;
                } else {
                    self.cache
                        .delete(&key)
                        .map_err(|e| InternalStringError(e.to_string()))?;
                    self.cache
                        .insert(&new)
                        .map_err(|e| InternalStringError(e.to_string()))?;
                }
            }
        };
        Ok(())
    }

    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        self.input_schemas = input_schemas.to_owned();
        Ok(())
    }
}

impl CacheSink {
    pub fn new(
        cache: Arc<LmdbCache>,
        api_endpoint: ApiEndpoint,
        input_schemas: HashMap<PortHandle, Schema>,
        schema_map: HashMap<u64, bool>,
        schema_change_notifier: Option<crossbeam::channel::Sender<bool>>,
    ) -> Self {
        Self {
            cache,
            counter: 0,
            before: Instant::now(),
            input_schemas,
            schema_map,
            api_endpoint,
            pb: get_progress(),
            schema_change_notifier,
        }
    }

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, ExecutionError> {
        let mut schema = schema.clone();
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
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {

    use crate::test_utils;

    use dozer_cache::cache::{index, Cache};

    use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
    use dozer_types::core::node::Sink;

    use dozer_types::types::{Field, Operation, Record, SchemaIdentifier};
    use std::panic;

    #[test]
    // This test cases covers updation of records when primary key changes because of value change in primary_key
    fn update_record_when_primary_changes() {
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
        let mut state = test_utils::init_state();

        sink.process(DEFAULT_PORT_HANDLE, 0_u64, insert_operation, state.as_mut())
            .unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(initial_values, record.values);

        sink.process(DEFAULT_PORT_HANDLE, 0_u64, update_operation, state.as_mut())
            .unwrap();

        // Primary key with old values
        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = panic::catch_unwind(|| {
            cache.get(&key).unwrap();
        });

        assert!(record.is_err());

        // Primary key with updated values
        let key = index::get_primary_key(&schema.primary_index, &updated_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(updated_values, record.values);
    }
}
