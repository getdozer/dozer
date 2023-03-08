use crate::connectors::delta_lake::DeltaLakeConnector;
use crate::connectors::Connector;
use crate::connectors::TableInfo;
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::ingestion_types::{DeltaLakeConfig, DeltaTable, IngestionMessageKind};
use dozer_types::types::SourceDefinition::Dynamic;
use dozer_types::types::{Field, FieldType, Operation};
use std::thread;

#[test]
fn get_schema_from_deltalake() {
    let path = "src/connectors/delta_lake/test/data/delta-0.8.0";
    let table_name = "test_table";
    let delta_table = DeltaTable {
        path: path.to_string(),
        name: table_name.to_string(),
    };
    let config = DeltaLakeConfig {
        tables: vec![delta_table],
    };

    let connector = DeltaLakeConnector::new(1, config);
    let table_info = TableInfo {
        name: table_name.to_string(),
        columns: None,
    };
    let field = connector.get_schemas(Some(vec![table_info])).unwrap()[0]
        .schema
        .fields[0]
        .clone();
    assert_eq!(&field.name, "value");
    assert_eq!(field.typ, FieldType::Int);
    assert!(field.nullable);
    assert_eq!(field.source, Dynamic);
}

#[test]
fn read_deltalake() {
    let path = "src/connectors/delta_lake/test/data/delta-0.8.0";
    let table_name = "test_table";
    let delta_table = DeltaTable {
        path: path.to_string(),
        name: table_name.to_string(),
    };
    let config = DeltaLakeConfig {
        tables: vec![delta_table],
    };

    let connector = DeltaLakeConnector::new(1, config);

    let config = IngestionConfig::default();
    let (ingestor, iterator) = Ingestor::initialize_channel(config);
    let table = TableInfo {
        name: "test_table".to_string(),
        columns: None,
    };
    thread::spawn(move || {
        let tables = vec![table];
        let _ = connector.start(None, &ingestor, tables);
    });

    let fields = vec![Field::Int(0), Field::Int(1), Field::Int(2), Field::Int(4)];
    let mut values = vec![];
    for (idx, IngestionMessage { identifier, kind }) in iterator.enumerate() {
        assert_eq!(idx, identifier.seq_in_tx as usize);
        if let IngestionMessageKind::OperationEvent(Operation::Insert { new }) = kind {
            values.extend(new.values);
        }
    }
    values.sort();
    assert_eq!(fields, values);
}
