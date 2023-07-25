use crate::connectors::object_store::connector::ObjectStoreConnector;
use crate::connectors::Connector;
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::ingestion_types::IngestionMessageKind;
use dozer_types::ingestion_types::LocalDetails;
use dozer_types::node::OpIdentifier;

use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::object_store::tests::test_utils::get_local_storage_config;
use crate::errors::ConnectorError::InitializationError;
use crate::errors::ObjectStoreObjectError;
use dozer_types::types::{Field, FieldType, Operation};

#[macro_export]
macro_rules! test_type_conversion {
    ($a:expr,$b:expr,$c:pat) => {
        let value = $a.get($b).unwrap();
        assert!(matches!(value, $c));
    };
}

#[tokio::test]
async fn test_get_schema_of_parquet() {
    let local_storage = get_local_storage_config("parquet", "");

    let connector = ObjectStoreConnector::new(local_storage);
    let (_, schemas) = connector.list_all_schemas().await.unwrap();
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

#[tokio::test]
async fn test_get_schema_of_csv() {
    let local_storage = get_local_storage_config("csv", "");

    let connector = ObjectStoreConnector::new(local_storage);
    let (_, schemas) = connector.list_all_schemas().await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_read_parquet_file() {
    let local_storage = get_local_storage_config("parquet", "");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();
    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    let mut i = 1;
    while i < 9 {
        let row = iterator.next();
        if let Some(IngestionMessage {
            identifier: OpIdentifier { seq_in_tx, .. },
            kind:
                IngestionMessageKind::OperationEvent {
                    op: Operation::Insert { new },
                    ..
                },
        }) = row
        {
            let values = new.values;

            assert_eq!(i, seq_in_tx);

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

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 9);
    } else {
        panic!("Unexpected message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_read_parquet_file_marker() {
    let local_storage = get_local_storage_config("parquet", "marker");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();
    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    let mut i = 1;
    while i < 9 {
        let row = iterator.next();
        if let Some(IngestionMessage {
            identifier: OpIdentifier { seq_in_tx, .. },
            kind:
                IngestionMessageKind::OperationEvent {
                    op: Operation::Insert { new },
                    ..
                },
        }) = row
        {
            let values = new.values;

            assert_eq!(i, seq_in_tx);

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

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 9);
    } else {
        panic!("Unexpected message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_read_parquet_file_no_marker() {
    let local_storage = get_local_storage_config("parquet", "no_marker");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();
    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 1);
    } else {
        panic!("Unexpected message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_csv_read() {
    let local_storage = get_local_storage_config("csv", "");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();

    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    let mut i = 1;
    while i < 21 {
        // no. of row in the csv data
        let row = iterator.next();
        if let Some(IngestionMessage {
            identifier: OpIdentifier { seq_in_tx, .. },
            kind:
                IngestionMessageKind::OperationEvent {
                    op: Operation::Insert { new },
                    ..
                },
        }) = row
        {
            let values = new.values;

            assert_eq!(i, seq_in_tx);
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
                if *id == 2 || *id == 12 {
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

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 21);
    } else {
        panic!("Unexpected message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_csv_read_marker() {
    let local_storage = get_local_storage_config("csv", "marker");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();

    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    let mut i = 1;
    while i < 11 {
        // no. of row in the csv data
        let row = iterator.next();
        if let Some(IngestionMessage {
            identifier: OpIdentifier { seq_in_tx, .. },
            kind:
                IngestionMessageKind::OperationEvent {
                    op: Operation::Insert { new },
                    ..
                },
        }) = row
        {
            let values = new.values;

            assert_eq!(i, seq_in_tx);
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
                if *id == 2 || *id == 12 {
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

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 11);
    } else {
        panic!("Unexpected message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_csv_read_only_one_marker() {
    let local_storage = get_local_storage_config("csv", "marker_only_one");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();

    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    let mut i = 1;
    while i < 11 {
        // no. of row in the csv data
        let row = iterator.next();
        if let Some(IngestionMessage {
            identifier: OpIdentifier { seq_in_tx, .. },
            kind:
                IngestionMessageKind::OperationEvent {
                    op: Operation::Insert { new },
                    ..
                },
        }) = row
        {
            let values = new.values;

            assert_eq!(i, seq_in_tx);
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
                if *id == 2 || *id == 12 {
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

    // No data to be snapshotted

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 11);
    } else {
        panic!("Unexpected message");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_csv_read_no_marker() {
    let local_storage = get_local_storage_config("csv", "no_marker");

    let connector = ObjectStoreConnector::new(local_storage);

    let config = IngestionConfig::default();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();

    tokio::spawn(async move {
        connector.start(&ingestor, tables).await.unwrap();
    });

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingStarted,
    }) = row
    {
        assert_eq!(seq_in_tx, 0);
    } else {
        panic!("Unexpected message");
    }

    // No data to be snapshotted

    let row = iterator.next();
    if let Some(IngestionMessage {
        identifier: OpIdentifier { seq_in_tx, .. },
        kind: IngestionMessageKind::SnapshottingDone,
    }) = row
    {
        assert_eq!(seq_in_tx, 1);
    } else {
        panic!("Unexpected message");
    }
}

#[test]
fn test_unsupported_format() {
    let local_storage = get_local_storage_config("unsupported", "");
    let table = local_storage.tables.get(0).unwrap();

    let result = map_listing_options(table);
    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(ObjectStoreObjectError::FileFormatUnsupportedError(_))
    ));
}

#[tokio::test]
async fn test_missing_directory() {
    let mut local_storage = get_local_storage_config("unsupported", "");
    local_storage.details = Some(LocalDetails {
        path: "not_existing_path".to_string(),
    });
    let connector = ObjectStoreConnector::new(local_storage);

    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await;

    assert!(tables.is_err());

    assert!(matches!(tables, Err(InitializationError(_))));
}
