use dozer_types::ingestion_types::{LocalStorage, S3Storage};
use std::collections::HashMap;

use crate::connectors::object_store::schema_mapper::{Mapper, SchemaMapper};
use crate::connectors::object_store::table_reader::{Reader, TableReader};
use crate::connectors::TableInfo;
use crate::errors::ConnectorError;
use crate::{connectors::Connector, errors, ingestion::Ingestor};

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
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(
        &self,
        _tables: &[crate::connectors::TableInfo],
    ) -> Result<crate::connectors::ValidationResults, errors::ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, ConnectorError> {
        let tables = table_names
            .as_ref()
            .map_or_else(std::vec::Vec::new, |t| t.clone());
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(tables)
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        let tables = tables.unwrap_or_default();

        let reader = TableReader::new(self.config.clone());

        reader.read_tables(tables, ingestor)
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }
}

impl Connector for ObjectStoreConnector<LocalStorage> {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(
        &self,
        _tables: &[crate::connectors::TableInfo],
    ) -> Result<crate::connectors::ValidationResults, errors::ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, ConnectorError> {
        let tables = table_names
            .as_ref()
            .map_or_else(std::vec::Vec::new, |t| t.clone());
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(tables)
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        let tables = tables.unwrap_or_default();

        let reader = TableReader::new(self.config.clone());

        reader.read_tables(tables, ingestor)
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }
}
