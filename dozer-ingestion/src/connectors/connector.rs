use dozer_types::types::{OperationEvent, Schema};
use std::{error::Error, sync::Arc};
use std::sync::Mutex;
use crate::connectors::seq_no_resolver::SeqNoResolver;
use super::storage::RocksStorage;

pub trait Connector: Send + Sync {
    fn get_schema(&self, name: String) -> anyhow::Result<Schema>;
    fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>>;
    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> Result<(), Box<dyn Error>>;
    fn iterator(&mut self, seq_no_resolver: Arc<Mutex<SeqNoResolver>>) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> anyhow::Result<()>;
}
