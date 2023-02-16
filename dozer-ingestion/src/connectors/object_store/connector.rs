use dozer_types::ingestion_types::{LocalStorage, S3Storage};
use std::collections::HashMap;
use std::sync::Arc;

use crate::connectors::object_store::schema_mapper::{Mapper, SchemaMapper};
use crate::connectors::object_store::table_reader::{Reader, TableReader};
use crate::connectors::TableInfo;
use crate::errors::ConnectorError;
use crate::{connectors::Connector, errors, ingestion::Ingestor};
use dozer_types::parking_lot::RwLock;

pub struct ObjectStoreConnector<T: Clone> {
    pub id: u64,
    config: T,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    tables: Option<Vec<TableInfo>>,
}

impl<T: Clone> ObjectStoreConnector<T> {
    pub fn new(id: u64, config: T) -> Self {
        Self {
            id,
            config,
            ingestor: None,
            tables: None,
        }
    }
}

impl Connector for ObjectStoreConnector<S3Storage> {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(
        &self,
        _tables: &[TableInfo],
    ) -> Result<crate::connectors::ValidationResults, errors::ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, ConnectorError> {
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(table_names)
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        self.tables = tables;
        Ok(())
    }

    fn start(&self, _from_seq: Option<(u64, u64)>) -> Result<(), ConnectorError> {
        let tables = match &self.tables {
            Some(tables) if !tables.is_empty() => tables,
            _ => return Ok(()),
        };

        let ingestor = self
            .ingestor
            .as_ref()
            .ok_or(ConnectorError::InitializationError)?;

        TableReader::new(self.config.clone()).read_tables(tables, ingestor)
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
        let mapper = SchemaMapper::new(self.config.clone());
        mapper.get_schema(table_names)
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        self.tables = tables;
        Ok(())
    }

    fn start(&self, _from_seq: Option<(u64, u64)>) -> Result<(), ConnectorError> {
        let tables = match &self.tables {
            Some(tables) if !tables.is_empty() => tables,
            _ => return Ok(()),
        };

        let ingestor = self
            .ingestor
            .as_ref()
            .ok_or(ConnectorError::InitializationError)?;

        TableReader::new(self.config.clone()).read_tables(tables, ingestor)
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }
}
