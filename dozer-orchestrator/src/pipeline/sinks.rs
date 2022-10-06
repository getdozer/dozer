use std::collections::HashMap;
use std::sync::Arc;

use dozer_cache::cache::lmdb::cache::LmdbCache;
use dozer_cache::cache::{get_primary_key, Cache};
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{NextStep, Sink, SinkFactory};
use dozer_core::state::StateStore;
use dozer_types::types::{Operation, OperationEvent, Schema};

pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<LmdbCache>,
}

impl CacheSinkFactory {
    pub fn new(input_ports: Vec<PortHandle>) -> Self {
        let cache = LmdbCache::new(true);
        Self {
            input_ports,
            cache: Arc::new(cache),
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
            input_schemas: HashMap::new(),
        })
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    input_schemas: HashMap<PortHandle, Schema>,
}

impl Sink for CacheSink {
    fn init(
        &mut self,
        _state_store: &mut dyn StateStore,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<()> {
        self.input_schemas = input_schemas.to_owned();
        println!("SINK: Initialising CacheSink");
        Ok(())
    }

    fn process(
        &self,
        from_port: PortHandle,
        op: OperationEvent,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        // println!("SINK: Message {} received", _op.seq_no);
        let schema = &self.input_schemas[&from_port].clone();
        match op.operation {
            Operation::Delete { old } => {
                let key = get_primary_key(schema.primary_index.clone(), old.values);
                self.cache.delete(key)?;
            }
            Operation::Insert { new } => {
                self.cache.insert(new.clone(), schema.clone())?;
                println!("Inserted: {:?}", new.clone());
            }
            Operation::Update { old, new } => {
                let key = get_primary_key(schema.primary_index.clone(), old.values);
                self.cache.update(key, new, schema.clone())?;
            }
            Operation::Terminate => {}
        };
        Ok(NextStep::Continue)
    }
}
