use dozer_ingestion::connectors::object_store::connector::ObjectStoreConnector;
use dozer_types::{
    ingestion_types::{LocalDetails, LocalStorage, Table},
    types::{Operation, Schema},
};
use tempdir::TempDir;

use crate::test_suite::ConnectorTest;

use super::super::arrow::{operations_to_arrow, schema_to_arrow};

pub struct LocalStorageObjectStoreConnectorTest {
    _temp_dir: TempDir,
    connector: ObjectStoreConnector<LocalStorage>,
}

impl ConnectorTest for LocalStorageObjectStoreConnectorTest {
    type Connector = ObjectStoreConnector<LocalStorage>;

    fn new(
        schema_name: Option<String>,
        table_name: String,
        schema: Schema,
        operations: Vec<Operation>,
    ) -> Option<(Self, Schema)> {
        if schema_name.is_some() {
            return None;
        }

        let record_batch = operations_to_arrow(&operations, schema.clone())?;

        let temp_dir = TempDir::new("local").expect("Failed to create temp dir");
        let path = temp_dir.path().join(&table_name);
        std::fs::create_dir_all(&path).expect("Failed to create dir");

        let file = std::fs::File::create(path.join("0.parquet")).expect("Failed to create file");
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            record_batch.schema(),
            Some(props),
        )
        .expect("Failed to create writer");
        writer
            .write(&record_batch)
            .expect("Failed to write record batch");
        writer.close().expect("Failed to close writer");

        let prefix = format!("/{table_name}");
        let local_storage = LocalStorage {
            details: Some(LocalDetails {
                path: temp_dir.path().to_str().expect("Non-UTF8 path").to_string(),
            }),
            tables: vec![Table {
                name: table_name,
                prefix,
                file_type: "parquet".to_string(),
                extension: ".parquet".to_string(),
            }],
        };
        let connector = ObjectStoreConnector::new(0, local_storage);

        let (_, schema) = schema_to_arrow(schema);

        Some((
            Self {
                _temp_dir: temp_dir,
                connector,
            },
            schema,
        ))
    }

    fn connector(&self) -> &Self::Connector {
        &self.connector
    }

    fn start(&self) {}
}
