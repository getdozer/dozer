use tonic::async_trait;

use crate::{connectors::TableInfo, errors::ConnectorError};

use super::{adapters::DozerObjectStore, table_reader::TableReader};

#[async_trait]
pub trait Watcher<T> {
    async fn watch(&self, tables: &[TableInfo]) -> Result<(), ConnectorError>;
}

#[async_trait]
impl<T: DozerObjectStore> Watcher<T> for TableReader<T> {
    async fn watch(&self, tables: &[TableInfo]) -> Result<(), ConnectorError> {
        Ok(())
    }
}
