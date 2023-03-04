use dozer_types::types::SourceSchema;
use std::collections::HashMap;
use std::fmt::Debug;

use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::schema_mapper::{Mapper, SchemaMapper};
use crate::connectors::object_store::table_reader::{Reader, TableReader};
use crate::connectors::{Connector, TableInfo, ValidationResults};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;

type ConnectorResult<T> = Result<T, ConnectorError>;

#[derive(Debug)]
pub struct ObjectStoreConnector<T: Clone> {
    pub id: u64,
    config: T,
}

impl<T: DozerObjectStore> ObjectStoreConnector<T> {
    pub fn new(id: u64, config: T) -> Self {
        Self { id, config }
    }
}

impl<T: DozerObjectStore> Connector for ObjectStoreConnector<T> {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> ConnectorResult<()> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> ConnectorResult<ValidationResults> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SourceSchema>> {
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(table_names)
    }

    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> ConnectorResult<()> {
        TableReader::new(self.config.clone()).read_tables(&tables, ingestor)
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> ConnectorResult<Vec<TableInfo>> {
        self.get_tables_default(tables)
    }
}
