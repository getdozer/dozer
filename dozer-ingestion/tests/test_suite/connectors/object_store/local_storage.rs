use dozer_ingestion::connectors::object_store::connector::ObjectStoreConnector;

use dozer_types::{
    arrow,
    ingestion_types::{LocalDetails, LocalStorage, ParquetConfig, Table, TableConfig},
    types::Field,
};
use tempdir::TempDir;
use tonic::async_trait;

use crate::test_suite::{DataReadyConnectorTest, FieldsAndPk, InsertOnlyConnectorTest};

use super::super::arrow::{
    record_batch_with_all_supported_data_types, records_to_arrow, schema_to_arrow,
};

pub struct LocalStorageObjectStoreConnectorTest {
    _temp_dir: TempDir,
}

#[async_trait]
impl DataReadyConnectorTest for LocalStorageObjectStoreConnectorTest {
    type Connector = ObjectStoreConnector<LocalStorage>;

    async fn new() -> (Self, Self::Connector) {
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

#[async_trait]
impl InsertOnlyConnectorTest for LocalStorageObjectStoreConnectorTest {
    type Connector = ObjectStoreConnector<LocalStorage>;

    async fn new(
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

    let local_storage = LocalStorage {
        details: Some(LocalDetails {
            path: temp_dir.path().to_str().expect("Non-UTF8 path").to_string(),
        }),
        tables: vec![Table {
            config: Some(TableConfig::Parquet(ParquetConfig {
                path: table_name.to_string(),
                extension: ".parquet".to_string(),
                marker_file: false,
            })),
            name: table_name,
        }],
    };
    let connector = ObjectStoreConnector::new(local_storage);

    (temp_dir, connector)
}
