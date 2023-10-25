use dozer_ingestion_connector::{
    async_trait,
    dozer_types::types::{Field, FieldDefinition},
    Connector,
};

#[async_trait]
pub trait DataReadyConnectorTest: Send + Sized + 'static {
    type Connector: Connector;

    async fn new() -> (Self, Self::Connector);
}

pub type FieldsAndPk = (Vec<FieldDefinition>, Vec<usize>);

#[async_trait]
pub trait InsertOnlyConnectorTest: Send + Sized + 'static {
    type Connector: Connector;

    /// Creates a connector which contains a table whose name is `table_name`.
    ///
    /// The test should try its best to create a table with the given `schema_name`, `table_name` and `schema`.
    /// If any of the field in `schema` is not supported, it can skip that field.
    ///
    /// If `schema_name` or `schema.primary_index` is not supported in this connector, `None` should be returned.
    ///
    /// The actually created schema should be returned.
    async fn new(
        schema_name: Option<String>,
        table_name: String,
        schema: FieldsAndPk,
        records: Vec<Vec<Field>>,
    ) -> Option<(Self, Self::Connector, FieldsAndPk)>;
}

#[async_trait]
pub trait CudConnectorTest: InsertOnlyConnectorTest {
    /// Spawns a thread to feed cud operations to connector.
    async fn start_cud(&self, operations: Vec<records::Operation>);
}

mod basic;
mod connectors;
mod data;
mod records;

pub use basic::{
    run_test_suite_basic_cud, run_test_suite_basic_data_ready, run_test_suite_basic_insert_only,
};

#[cfg(feature = "mongodb")]
pub use connectors::MongodbConnectorTest;

pub use connectors::{
    DozerConnectorTest, LocalStorageObjectStoreConnectorTest, PostgresConnectorTest,
};
