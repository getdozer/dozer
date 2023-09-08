use crate::connectors::delta_lake::DeltaLakeConnector;
use crate::connectors::Connector;
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::ingestion_types::{DeltaLakeConfig, DeltaTable};
use dozer_types::types::SourceDefinition::Dynamic;
use dozer_types::types::{Field, FieldType, Operation};

#[tokio::test]
async fn get_schema_from_deltalake() {
    let path = "src/connectors/delta_lake/test/data/delta-0.8.0";
    let table_name = "test_table";
    let delta_table = DeltaTable {
        path: path.to_string(),
        name: table_name.to_string(),
    };
    let config = DeltaLakeConfig {
        tables: vec![delta_table],
    };

    let connector = DeltaLakeConnector::new(config);
    let (_, schemas) = connector.list_all_schemas().await.unwrap();
    let field = schemas[0].schema.fields[0].clone();
    assert_eq!(&field.name, "value");
    assert_eq!(field.typ, FieldType::Int);
    assert!(field.nullable);
    assert_eq!(field.source, Dynamic);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn read_deltalake() {
    let path = "src/connectors/delta_lake/test/data/delta-0.8.0";
    let table_name = "test_table";
    let delta_table = DeltaTable {
        path: path.to_string(),
        name: table_name.to_string(),
    };
    let config = DeltaLakeConfig {
        tables: vec![delta_table],
    };

    let connector = DeltaLakeConnector::new(config);

    let config = IngestionConfig::default();
    let (ingestor, iterator) = Ingestor::initialize_channel(config);
    let tables = connector
        .list_columns(connector.list_tables().await.unwrap())
        .await
        .unwrap();
    tokio::spawn(async move { connector.start(&ingestor, tables).await.unwrap() });

    let fields = vec![Field::Int(0), Field::Int(1), Field::Int(2), Field::Int(4)];
    let mut values = vec![];
    for message in iterator {
        if let IngestionMessage::OperationEvent {
            op: Operation::Insert { new },
            ..
        } = message
        {
            values.extend(new.values);
        }
    }
    values.sort();
    assert_eq!(fields, values);
}
