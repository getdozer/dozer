use dozer_ingestion::connectors::Connector;
use dozer_types::types::{Operation, Schema};

pub trait ConnectorTest: Send + Sized + 'static {
    type Connector: Connector;

    /// Creates a connector which contains a table whose name is `table_name`.
    ///
    /// The test should try its best to create a table with the given `schema_name`, `table_name`, `schema` and `operations`.
    /// If any of the field in `schema` is not supported, it can skip that field.
    ///
    /// If `schema_name` is not supported in this connector, or any operation cannot be applied to the table, `None` should be returned.
    ///
    /// The actual created schema should be returned.
    fn new(
        schema_name: Option<String>,
        table_name: String,
        schema: Schema,
        operations: Vec<Operation>,
    ) -> Option<(Self, Schema)>;

    fn connector(&self) -> &Self::Connector;

    /// Starts feeding data to the connector.
    fn start(&self);
}

mod basic;
mod connectors;
mod data;

pub use basic::run_test_suite_basic;
pub use connectors::LocalStorageObjectStoreConnectorTest;
