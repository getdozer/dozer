use crate::connectors::delta_lake::DeltaLakeConnector;
use crate::connectors::Connector;
use crate::connectors::TableInfo;
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::{DeltaLakeConfig, DeltaTable};
use dozer_types::types::{Field, Operation};
use std::thread;

#[test]
fn get_schema_from_deltalake() {}

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
    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);
    let table = TableInfo {
        name: "test_table".to_string(),
        table_name: "test_table".to_string(),
        id: 0,
        columns: None,
    };
    thread::spawn(move || {
        let tables = vec![table];
        let _ = connector.start(None, &ingestor, tables);
    });

    let mut idx = 0;
    let fields = vec![Field::Int(0), Field::Int(1), Field::Int(2), Field::Int(4)];
    while let Some(((_, seq_no), Operation::Insert { new })) = iterator.next() {
        assert_eq!(idx, seq_no as usize);
        let values = new.values;
        assert_eq!(values, vec![fields[idx].clone()]);
        idx += 1;
    }
}
