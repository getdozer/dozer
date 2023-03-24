use dozer_ingestion::connectors::Connector;
use dozer_types::types::{Operation, Record, Schema};

pub trait DataReadyConnectorTest: Send + Sized + 'static {
    type Connector: Connector;

    fn new() -> Self;

    fn connector(&self) -> &Self::Connector;
}

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
    fn new(
        schema_name: Option<String>,
        table_name: String,
        schema: Schema,
        records: Vec<Record>,
    ) -> Option<(Self, Schema)>;

    fn connector(&self) -> &Self::Connector;
}

pub trait CrudConnectorTest: InsertOnlyConnectorTest {
    fn start_crud(&self, operations: Vec<Operation>);
}

mod basic;
mod connectors;
mod data;

pub use basic::{run_test_suite_basic_data_ready, run_test_suite_basic_insert_only};
pub use connectors::LocalStorageObjectStoreConnectorTest;
