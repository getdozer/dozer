use dozer_ingestion::connectors::object_store::connector::ObjectStoreConnector;
use dozer_types::arrow_types::from_arrow::map_schema_to_dozer;
use dozer_types::arrow_types::to_arrow::map_to_arrow_schema;
use dozer_types::{
    arrow,
    ingestion_types::{LocalDetails, LocalStorage, Table},
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

#[test]
fn roundtrip_record_to_record_batch() {
    use dozer_types::arrow::record_batch::RecordBatch;
    use dozer_types::arrow_types::from_arrow::map_record_batch_to_dozer_records;
    use dozer_types::arrow_types::to_arrow::map_record_to_arrow;
    use dozer_types::json_types::JsonValue;
    use dozer_types::ordered_float::OrderedFloat;
    use dozer_types::types::{FieldDefinition, FieldType, Record, Schema, SourceDefinition};

    let id = FieldDefinition::new(
        "id".to_string(),
        FieldType::Int,
        false,
        SourceDefinition::Dynamic,
    );
    let name = FieldDefinition::new(
        "name".to_string(),
        FieldType::String,
        false,
        SourceDefinition::Dynamic,
    );
    let json = FieldDefinition::new(
        "json".to_string(),
        FieldType::Json,
        false,
        SourceDefinition::Dynamic,
    );

    let schema: Schema = Schema::empty()
        .field(id, false)
        .field(name, false)
        .field(json, false)
        .clone();

    let records: Vec<Field> = vec![
        Field::Int(1),
        Field::String("test".to_string()),
        Field::Json(JsonValue::Array(vec![
            JsonValue::Number(OrderedFloat(1_f64)),
            JsonValue::Number(OrderedFloat(2_f64)),
            JsonValue::Number(OrderedFloat(3_f64)),
        ])),
    ];

    let record: Record = Record::new(None, records);
    let record_batch: RecordBatch = map_record_to_arrow(record.clone(), &schema).unwrap();
    let res: Vec<Record> = map_record_batch_to_dozer_records(record_batch, &schema).unwrap();

    assert_eq!(vec![record], res);

    let arrow_schema = map_to_arrow_schema(&schema).unwrap();
    let original_schema = map_schema_to_dozer(&arrow_schema).unwrap();

    assert_eq!(original_schema, schema);
}
