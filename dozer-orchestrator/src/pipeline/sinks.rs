use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

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
    pub fn new(input_ports: Vec<PortHandle>, cache: Arc<LmdbCache>) -> Self {
        Self { input_ports, cache }
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
        })
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    counter: i32,
    before: Instant,
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
        &mut self,
        from_port: PortHandle,
        op: OperationEvent,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        // println!("SINK: Message {} received", _op.seq_no);
        self.counter = self.counter + 1;
        const BACKSPACE: char = 8u8 as char;
        if self.counter % 10 == 0 {
            print!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                self.counter,
                self.before.elapsed(),
            );
        }
        let schema = &self.input_schemas[&from_port].clone();
        match op.operation {
            Operation::Delete { old } => {
                let key = get_primary_key(schema.primary_index.clone(), old.values);
                self.cache.delete(key)?;
            }
            Operation::Insert { new } => {
                self.cache.insert(new.clone(), schema.clone())?;
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
