use crate::DeltaLakeConnector;
use dozer_ingestion_connector::{
    dozer_types::{
        models::ingestion_types::{DeltaLakeConfig, DeltaTable, IngestionMessage},
        types::{Field, FieldType, Operation, SourceDefinition},
    },
    test_util::create_runtime_and_spawn_connector_all_tables,
    tokio, Connector,
};

#[tokio::test]
async fn get_schema_from_deltalake() {
    let path = "src/test/data/delta-0.8.0";
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
    assert_eq!(field.source, SourceDefinition::Dynamic);
}

#[test]
fn read_deltalake() {
    let path = "src/test/data/delta-0.8.0";
    let table_name = "test_table";
    let delta_table = DeltaTable {
        path: path.to_string(),
        name: table_name.to_string(),
    };
    let config = DeltaLakeConfig {
        tables: vec![delta_table],
    };

    let connector = DeltaLakeConnector::new(config);

    let (iterator, _) = create_runtime_and_spawn_connector_all_tables(connector);

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
