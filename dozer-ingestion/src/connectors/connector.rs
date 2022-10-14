use dozer_types::{
    errors::connector::ConnectorError,
    types::{OperationEvent, Schema},
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use super::{seq_no_resolver::SeqNoResolver, storage::RocksStorage};
pub trait Connector: Send + Sync {
    fn get_schema(&self, name: String) -> Result<Schema, ConnectorError>;
    fn get_all_schema(&self) -> Result<Vec<(String, Schema)>, ConnectorError>;
    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError>;
    fn initialize(
        &mut self,
        storage_client: Arc<RocksStorage>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError>;
    fn iterator(
        &mut self,
        seq_no_resolver: Arc<Mutex<SeqNoResolver>>,
    ) -> Box<dyn Iterator<Item = OperationEvent> + 'static>;
    fn stop(&self);
    fn test_connection(&self) -> Result<(), ConnectorError>;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableInfo {
    pub name: String,
    pub id: u32,
    pub columns: Option<Vec<String>>,
}
