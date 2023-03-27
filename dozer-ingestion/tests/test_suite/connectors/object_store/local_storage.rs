use dozer_ingestion::connectors::object_store::connector::ObjectStoreConnector;
use dozer_types::{
    arrow,
    ingestion_types::{LocalDetails, LocalStorage, Table},
    types::Field,
};
use tempdir::TempDir;

use crate::test_suite::{DataReadyConnectorTest, FieldsAndPk, InsertOnlyConnectorTest};

use super::super::arrow::{
    record_batch_with_all_supported_data_types, records_to_arrow, schema_to_arrow,
};

pub struct LocalStorageObjectStoreConnectorTest {
    _temp_dir: TempDir,
}

impl DataReadyConnectorTest for LocalStorageObjectStoreConnectorTest {
    type Connector = ObjectStoreConnector<LocalStorage>;

    fn new() -> (Self, Self::Connector) {
        let record_batch = record_batch_with_all_supported_data_types();
        let (temp_dir, connector) = create_connector("sample".to_string(), &record_batch);
        (
            Self {
                _temp_dir: temp_dir,
            },
            connector,
        )
    }
}

impl InsertOnlyConnectorTest for LocalStorageObjectStoreConnectorTest {
    type Connector = ObjectStoreConnector<LocalStorage>;

    fn new(
        schema_name: Option<String>,
        table_name: String,
        schema: FieldsAndPk,
        records: Vec<Vec<Field>>,
    ) -> Option<(Self, Self::Connector, FieldsAndPk)> {
        if schema_name.is_some() {
            return None;
        }

        let record_batch = records_to_arrow(&records, schema.0.clone());

        let (temp_dir, connector) = create_connector(table_name, &record_batch);

        let (_, schema) = schema_to_arrow(schema.0);

        Some((
            Self {
                _temp_dir: temp_dir,
            },
            connector,
            schema,
        ))
    }
}

fn create_connector(
    table_name: String,
    record_batch: &arrow::record_batch::RecordBatch,
) -> (TempDir, ObjectStoreConnector<LocalStorage>) {
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
        .write(record_batch)
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

    (temp_dir, connector)
}
