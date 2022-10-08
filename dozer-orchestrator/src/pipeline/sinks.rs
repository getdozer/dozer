use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use dozer_cache::cache::lmdb::cache::LmdbCache;
use dozer_cache::cache::{get_primary_key, Cache};
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{NextStep, Sink, SinkFactory};
use dozer_core::state::StateStore;
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::types::{Operation, OperationEvent, Schema};

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
        })
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    counter: i32,
    before: Instant,
    input_schemas: HashMap<PortHandle, Schema>,
    api_endpoint: ApiEndpoint,
}

impl Sink for CacheSink {
    fn init(&mut self, _state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        println!("SINK: Initialising CacheSink");
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: OperationEvent,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        // println!("SINK: Message {} received", _op.seq_no);
        self.counter = self.counter + 1;
        const BACKSPACE: char = 8u8 as char;
        if self.counter % 1000 == 0 {
            print!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                self.counter,
                self.before.elapsed(),
            );
        }

        let schema = self.get_output_schema(&self.input_schemas[&from_port])?;

        match op.operation {
            Operation::Delete { old } => {
                let key = get_primary_key(&schema.primary_index, &old.values);
                self.cache.delete(&key)?;
            }
            Operation::Insert { new } => {
                let mut new = new.clone();
                new.schema_id = schema.identifier.clone();

                self.cache
                    .insert_with_schema(&new, &schema, &self.api_endpoint.name)?;
            }
            Operation::Update { old, new } => {
                let key = get_primary_key(&schema.primary_index, &old.values);
                let mut new = new.clone();
                new.schema_id = schema.identifier.clone();
                self.cache.update(&key, &new, &schema)?;
            }
            Operation::Terminate => {}
            Operation::SchemaUpdate { new } => {}
        };
        Ok(NextStep::Continue)
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
