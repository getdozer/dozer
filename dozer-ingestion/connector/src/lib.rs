use std::fmt::Debug;

use dozer_types::errors::internal::BoxedError;
use dozer_types::models::source::ReplicationMode;
use dozer_types::node::OpIdentifier;
use dozer_types::serde;
use dozer_types::serde::{Deserialize, Serialize};
pub use dozer_types::tonic::async_trait;
use dozer_types::types::{FieldType, Schema};

mod ingestor;
pub mod schema_parser;
pub mod test_util;
pub mod utils;

pub use ingestor::{IngestionConfig, IngestionIterator, Ingestor};

pub use dozer_types;
pub use futures;
pub use tokio;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
#[serde(crate = "dozer_types::serde")]
/// A source table's CDC event type.
pub enum CdcType {
    /// Connector gets old record on delete/update operations.
    FullChanges,
    /// Connector only gets PK of old record on delete/update operations.
    OnlyPK,
    #[default]
    /// Connector cannot get any info about old records. In other words, the table is append-only.
    Nothing,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(crate = "dozer_types::serde")]
/// A source table's schema and CDC type.
pub struct SourceSchema {
    /// Dozer schema mapped from the source table. Columns are already filtered based on `TableInfo.column_names`.
    pub schema: Schema,
    #[serde(default)]
    /// The source table's CDC type.
    pub cdc_type: CdcType,
}

impl SourceSchema {
    pub fn new(schema: Schema, cdc_type: CdcType) -> Self {
        Self { schema, cdc_type }
    }
}

/// Result of mapping one source table schema to Dozer schema.
pub type SourceSchemaResult = Result<SourceSchema, BoxedError>;

#[async_trait]
pub trait Connector: Send + Sync + Debug {
    /// Returns all the external types and their corresponding Dozer types.
    /// If the external type is not supported, None should be returned.
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized;

    /// Validates the connector's connection level properties.
    async fn validate_connection(&mut self) -> Result<(), BoxedError>;

    /// Lists all the table names in the connector.
    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError>;

    /// Validates the connector's table level properties for each table.
    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError>;

    /// Lists all the column names for each table.
    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError>;

    /// Gets the schema for each table. Only requested columns need to be mapped.
    ///
    /// If this function fails at the connector level, such as a network error, it should return a outer level `Err`.
    /// Otherwise the outer level `Ok` should always contain the same number of elements as `table_infos`.
    ///
    /// If it fails at the table or column level, such as a unsupported data type, one of the elements should be `Err`.
    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError>;

    /// Lists all tables and columns and gets the schema for each table.
    async fn list_all_schemas(
        &mut self,
    ) -> Result<(Vec<TableInfo>, Vec<SourceSchema>), BoxedError> {
        let tables = self.list_tables().await?;
        let table_infos = self.list_columns(tables).await?;
        let schemas = self
            .get_schemas(&table_infos)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok((table_infos, schemas))
    }

    /// Serializes any state that's required to re-instantiate this connector. Should not be confused with `last_checkpoint`.
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError>;

    /// Starts outputting data from `tables` to `ingestor`. This method should never return unless there is an unrecoverable error.
    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError>;

    async fn start_with_replicationmode(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<ReplicationTable>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let mut simple_tables = Vec::new();
        for table in tables {
            if table.replication_mode != ReplicationMode::Full {
                return Err(format!(
                    "Replication mode {:?} is not supported for this connector",
                    table.replication_mode
                )
                .into());
            }
            simple_tables.push(table.info);
        }
        self.start(ingestor, simple_tables, last_checkpoint).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Unique identifier of a source table. A source table must have a `name`, optionally under a `schema` scope.
pub struct TableIdentifier {
    /// The `schema` scope of the table.
    ///
    /// Connector that supports schema scope must decide on a default schema, that doesn't must assert that `schema.is_none()`.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
}

impl TableIdentifier {
    pub fn new(schema: Option<String>, name: String) -> Self {
        Self { schema, name }
    }

    pub fn from_table_name(name: String) -> Self {
        Self { schema: None, name }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "self::serde")]
/// `TableIdentifier` with column names.
pub struct TableInfo {
    /// The `schema` scope of the table.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
    /// The column names to be mapped.
    pub column_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ReplicationTable {
    pub info: TableInfo,
    pub replication_mode: ReplicationMode,
}
