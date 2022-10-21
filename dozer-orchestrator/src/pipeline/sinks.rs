use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use dozer_cache::cache::LmdbCache;
use dozer_cache::cache::{index, Cache};
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{Sink, SinkFactory};
use dozer_core::state::{StateStore, StateStoreOptions};
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::types::{IndexDefinition, Operation, Schema, SchemaIdentifier};
use log::debug;

pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<LmdbCache>,
    api_endpoint: ApiEndpoint,
}

impl CacheSinkFactory {
    pub fn new(
        input_ports: Vec<PortHandle>,
        cache: Arc<LmdbCache>,
        api_endpoint: ApiEndpoint,
    ) -> Self {
        Self {
            input_ports,
            cache,
            api_endpoint,
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
        Box::new(CacheSink {
            cache: self.cache.clone(),
            counter: 0,
            before: Instant::now(),
            input_schemas: HashMap::new(),
            api_endpoint: self.api_endpoint.clone(),
            schema_map: HashMap::new(),
        })
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    counter: i32,
    before: Instant,
    input_schemas: HashMap<PortHandle, Schema>,
    schema_map: HashMap<u64, bool>,
    api_endpoint: ApiEndpoint,
}

impl Sink for CacheSink {
    fn init(&mut self, _state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        debug!("SINK: Initialising CacheSink");
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        _seq: u64,
        op: Operation,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<()> {
        // debug!("SINK: Message {} received", _op.seq_no);
        self.counter += 1;
        const BACKSPACE: char = 8u8 as char;
        if self.counter % 1000 == 0 {
            print!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                self.counter,
                self.before.elapsed(),
            );
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
            .map(|(idx, _f)| IndexDefinition {
                fields: vec![idx],
                sort_direction: vec![true],
                typ: dozer_types::types::IndexType::SortedInverted,
            })
            .collect();

        // Insert if schema not already inserted
        if let std::collections::hash_map::Entry::Vacant(e) = self.schema_map.entry(hash) {
            self.cache.insert_schema(&self.api_endpoint.name, &schema)?;
            e.insert(true);
        }

        match op {
            Operation::Delete { old } => {
                let key = index::get_primary_key(&schema.primary_index, &old.values);
                self.cache.delete(&key)?;
            }
            Operation::Insert { new } => {
                let mut new = new;
                new.schema_id = schema.identifier;

                self.cache.insert(&new)?;
            }
            Operation::Update { old, new } => {
                let key = index::get_primary_key(&schema.primary_index, &old.values);
                let mut new = new;
                new.schema_id = schema.identifier.clone();
                if index::has_primary_key_changed(&schema.primary_index, &old.values, &new.values) {
                    self.cache.update(&key, &new, &schema)?;
                } else {
                    self.cache.delete(&key)?;
                    self.cache.insert(&new)?;
                }
            }
        };
        Ok(())
    }

    fn update_schema(&mut self, input_schemas: &HashMap<PortHandle, Schema>) -> anyhow::Result<()> {
        self.input_schemas = input_schemas.to_owned();
        Ok(())
    }
}

impl CacheSink {
    fn get_output_schema(&self, schema: &Schema) -> anyhow::Result<Schema> {
        let mut schema = schema.clone();
        let api_index = &self.api_endpoint.index;
        let mut primary_index = Vec::new();
        for name in api_index.primary_key.iter() {
            let idx = schema
                .fields
                .iter()
                .position(|fd| fd.name == name.clone())
                .context("column_name not available in index.primary_keys")?;
            primary_index.push(idx);
        }
        schema.primary_index = primary_index;
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::CacheSink;
    use dozer_cache::cache::lmdb::cache::LmdbCache;
    use dozer_cache::cache::{index, Cache};
    use dozer_core::dag::dag::PortHandle;
    use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
    use dozer_core::dag::node::Sink;
    use dozer_core::state::lmdb::LmdbStateStoreManager;
    use dozer_core::state::StateStoresManager;
    use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SchemaIdentifier,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;
    use std::{fs, panic};
    use tempdir::TempDir;

    #[test]
    fn update_record() {
        let cache = Arc::new(LmdbCache::new(true));

        let schema = Schema {
            identifier: Option::from(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![
                FieldDefinition {
                    name: "film_id".to_string(),
                    typ: FieldType::Int,
                    nullable: false,
                },
                FieldDefinition {
                    name: "film_name".to_string(),
                    typ: FieldType::String,
                    nullable: false,
                },
            ],
            values: vec![0],
            primary_index: vec![0],
            secondary_indexes: vec![],
        };

        let mut schema_map: HashMap<u64, bool> = HashMap::new();
        schema_map.insert(1, true);

        let mut input_schemas: HashMap<PortHandle, Schema> = HashMap::new();
        input_schemas.insert(DEFAULT_PORT_HANDLE, schema.clone());

        let mut sink = CacheSink {
            cache: Arc::clone(&cache),
            counter: 0,
            before: Instant::now(),
            input_schemas,
            schema_map,
            api_endpoint: ApiEndpoint {
                id: None,
                name: "films".to_string(),
                path: "/films".to_string(),
                enable_rest: false,
                enable_grpc: false,
                sql: "SELECT film_name FROM film WHERE 1=1".to_string(),
                index: ApiIndex {
                    primary_key: vec!["film_id".to_string()],
                },
            },
        };

        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
        if tmp_dir.path().exists() {
            fs::remove_dir_all(tmp_dir.path())
                .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
        }
        fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        let sm = LmdbStateStoreManager::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            1024 * 1024 * 1024 * 5,
            20_000,
        );
        let mut state = sm.init_state_store("1".to_string()).unwrap();

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

        sink.process(DEFAULT_PORT_HANDLE, 0_u64, insert_operation, state.as_mut())
            .unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(initial_values, record.values);

        sink.process(DEFAULT_PORT_HANDLE, 0_u64, update_operation, state.as_mut())
            .unwrap();
        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = panic::catch_unwind(|| {
            cache.get(&key).unwrap();
        });

        assert!(record.is_err());

        let key = index::get_primary_key(&schema.primary_index, &updated_values);
        let record = cache.get(&key).unwrap();

        assert_eq!(updated_values, record.values);
    }
}
