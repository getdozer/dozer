use dozer_cache::cache::lmdb::cache::LmdbCache;
use dozer_cache::cache::{get_primary_key, Cache};
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{NextStep, Sink, SinkFactory};
use dozer_core::state::StateStore;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_types::types::{Operation, OperationEvent, Schema};
use std::collections::HashMap;
use std::sync::Arc;
pub struct CacheSinkFactory {
    input_ports: Vec<PortHandle>,
    cache: Arc<LmdbCache>,
}

impl CacheSinkFactory {
    pub fn new(input_ports: Vec<PortHandle>, schema_client: Arc<SchemaRegistryClient>) -> Self {
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
            input_schemas: None,
        })
    }
}

pub struct CacheSink {
    cache: Arc<LmdbCache>,
    input_schemas: Option<HashMap<PortHandle, Schema>>,
}

impl Sink for CacheSink {
    fn init(
        &self,
        state_store: &mut dyn StateStore,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<()> {
        self.input_schemas = Some(input_schemas);
        println!("SINK: Initialising CacheSink");
        Ok(())
    }

    fn process(
        &self,
        from_port: PortHandle,
        op: OperationEvent,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        //    println!("SINK: Message {} received", _op.seq_no);
        let schema = self.input_schemas.unwrap()[&from_port];
        match op.operation {
            Operation::Delete { old } => {
                let key = get_primary_key(schema.primary_index, old.values);
                self.cache.delete(key);
            }
            Operation::Insert { new } => {
                self.cache.insert(new, schema);
            }
            Operation::Update { old, new } => {
                let key = get_primary_key(schema.primary_index, old.values);
                self.cache.update(key, new, schema);
            }
            Operation::Terminate => {}
        };
        Ok(NextStep::Continue)
    }
}
