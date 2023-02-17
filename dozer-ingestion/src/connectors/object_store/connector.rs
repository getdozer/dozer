use dozer_types::ingestion_types::{LocalStorage, S3Storage};
use dozer_types::types::SchemaWithChangesType;
use std::collections::HashMap;

use crate::connectors::object_store::schema_mapper::{Mapper, SchemaMapper};
use crate::connectors::object_store::table_reader::{Reader, TableReader};
use crate::connectors::{Connector, TableInfo, ValidationResults};
use crate::ingestion::Ingestor;

type ConnectorResult<T> = Result<T, crate::errors::ConnectorError>;

pub struct ObjectStoreConnector<T: Clone> {
    pub id: u64,
    config: T,
}

impl<T: Clone> ObjectStoreConnector<T> {
    pub fn new(id: u64, config: T) -> Self {
        Self { id, config }
    }
}

impl Connector for ObjectStoreConnector<S3Storage> {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> ConnectorResult<()> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> ConnectorResult<ValidationResults> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SchemaWithChangesType>> {
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(table_names)
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<()> {
        let tables = match tables {
            Some(tables) if !tables.is_empty() => tables,
            _ => return Ok(()),
        };

        TableReader::new(self.config.clone()).read_tables(&tables, ingestor)
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> ConnectorResult<Vec<TableInfo>> {
        todo!()
    }
}

impl Connector for ObjectStoreConnector<LocalStorage> {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> ConnectorResult<()> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> ConnectorResult<ValidationResults> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SchemaWithChangesType>> {
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(table_names)
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<()> {
        let tables = match tables {
            Some(tables) if !tables.is_empty() => tables,
            _ => return Ok(()),
        };

        TableReader::new(self.config.clone()).read_tables(&tables, ingestor)
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> ConnectorResult<Vec<TableInfo>> {
        self.get_tables_default(tables)
    }
}
