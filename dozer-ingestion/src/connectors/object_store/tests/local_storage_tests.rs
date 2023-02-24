use crate::connectors::object_store::connector::ObjectStoreConnector;
use crate::connectors::Connector;
use crate::connectors::TableInfo;
use crate::ingestion::{IngestionConfig, Ingestor};
use std::thread;

use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::object_store::tests::test_utils::get_local_storage_config;
use crate::errors::ObjectStoreObjectError;
use dozer_types::types::{Field, FieldType, Operation};

#[macro_export]
macro_rules! test_type_conversion {
    ($a:expr,$b:expr,$c:pat) => {
        let value = $a.get($b).unwrap();
        assert!(matches!(value, $c));
    };
}

#[test]
fn test_get_schema_of_parquet() {
    let local_storage = get_local_storage_config("parquet");

    let connector = ObjectStoreConnector::new(1, local_storage);
    let schemas = connector.get_schemas(None).unwrap();
    let schema = schemas.get(0).unwrap();

    let fields = schema.schema.fields.clone();
    assert_eq!(fields.get(0).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(1).unwrap().typ, FieldType::Boolean);
    assert_eq!(fields.get(2).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(3).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(4).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(5).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(6).unwrap().typ, FieldType::Float);
    assert_eq!(fields.get(7).unwrap().typ, FieldType::Float);
    assert_eq!(fields.get(8).unwrap().typ, FieldType::Binary);
    assert_eq!(fields.get(9).unwrap().typ, FieldType::Binary);
    assert_eq!(fields.get(10).unwrap().typ, FieldType::Timestamp);
}

#[test]
fn test_get_schema_of_csv() {
    let local_storage = get_local_storage_config("csv");

    let connector = ObjectStoreConnector::new(1, local_storage);
    let schemas = connector.get_schemas(None).unwrap();
    let schema = schemas.get(0).unwrap();

    let fields = schema.schema.fields.clone();
    assert_eq!(fields.get(0).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(1).unwrap().typ, FieldType::String);
    assert_eq!(fields.get(2).unwrap().typ, FieldType::String);
    assert_eq!(fields.get(3).unwrap().typ, FieldType::Int);
    assert_eq!(fields.get(4).unwrap().typ, FieldType::Float);
    assert_eq!(fields.get(5).unwrap().typ, FieldType::Float);
    assert_eq!(fields.get(6).unwrap().typ, FieldType::Float);
    assert_eq!(fields.get(7).unwrap().typ, FieldType::String);
    assert_eq!(fields.get(8).unwrap().typ, FieldType::String);
}

#[test]
fn test_read_parquet_file() {
    let local_storage = get_local_storage_config("parquet");

    let connector = ObjectStoreConnector::new(1, local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let table = TableInfo {
        name: "all_types_parquet".to_string(),
        table_name: "all_types_parquet".to_string(),
        id: 0,
        columns: None,
    };
    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![table];

        let _ = connector.start(None, &ingestor, Some(tables));
    });

    let mut i = 0;
    while i < 8 {
        let row = iterator.next();
        if let Some(((_, seq_no), Operation::Insert { new })) = row {
            let values = new.values;

            assert_eq!(i, seq_no);

            test_type_conversion!(values, 0, Field::Int(_));
            test_type_conversion!(values, 1, Field::Boolean(_));
            test_type_conversion!(values, 2, Field::Int(_));
            test_type_conversion!(values, 3, Field::Int(_));
            test_type_conversion!(values, 4, Field::Int(_));
            test_type_conversion!(values, 5, Field::Int(_));
            test_type_conversion!(values, 6, Field::Float(_));
            test_type_conversion!(values, 7, Field::Float(_));
            test_type_conversion!(values, 8, Field::Binary(_));
            test_type_conversion!(values, 9, Field::Binary(_));
            test_type_conversion!(values, 10, Field::Timestamp(_));
        } else {
            panic!("Unexpected message");
        }

        i += 1;
    }
}

#[test]
fn test_csv_read() {
    let local_storage = get_local_storage_config("csv");

    let connector = ObjectStoreConnector::new(1, local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let table = TableInfo {
        name: "all_types_csv".to_string(),
        table_name: "all_types_csv".to_string(),
        id: 0,
        columns: None,
    };

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![table];

        let _ = connector.start(None, &ingestor, Some(tables));
    });

    let mut i = 0;
    while i < 9 {
        let row = iterator.next();
        if let Some(((_, seq_no), Operation::Insert { new })) = row {
            eprintln!("{} {}", i, seq_no);
            let values = new.values;

            assert_eq!(i, seq_no);
            test_type_conversion!(values, 0, Field::Int(_));
            test_type_conversion!(values, 1, Field::String(_));
            test_type_conversion!(values, 2, Field::String(_));
            test_type_conversion!(values, 3, Field::Int(_));
            test_type_conversion!(values, 4, Field::Float(_));
            test_type_conversion!(values, 5, Field::Float(_));
            test_type_conversion!(values, 6, Field::Float(_));
            test_type_conversion!(values, 7, Field::String(_));
            test_type_conversion!(values, 8, Field::String(_));

            if let Field::Int(id) = values.get(0).unwrap() {
                if *id < 3 {
                    test_type_conversion!(values, 9, Field::Float(_));
                } else {
                    test_type_conversion!(values, 9, Field::Null);
                }
            }
        } else {
            panic!("Unexpected message");
        }

        i += 1;
    }
}

#[test]
fn test_unsupported_format() {
    let local_storage = get_local_storage_config("unsupported");
    let table = local_storage.tables.get(0).unwrap();

    let result = map_listing_options(table);
    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(ObjectStoreObjectError::FileFormatUnsupportedError(_))
    ));
}
