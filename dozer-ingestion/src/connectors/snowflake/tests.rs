use crate::connectors::snowflake::test_utils::remove_streams;
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::IngestionOperation;
use std::thread;

#[ignore]
#[test]
fn connector_e2e_connect_and_read_from_snowflake_stream() {
    use dozer_types::models::source::Source;

    let source = serde_yaml::from_str::<Source>(&include_str!(
        "../../../../config/tests/test.snowflake.yaml"
    ))
    .unwrap();
    remove_streams(source.connection.clone(), &source.table_name).unwrap();

    let config = IngestionConfig::default();

    let (ingestor, iterator) = Ingestor::initialize_channel(config);

    thread::spawn(|| {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: source.table_name,
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(source.connection).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start().unwrap();
    });

    let mut i = 0;
    while i < 1000 {
        i += 1;
        let op = iterator.write().next();
        match op {
            None => {}
            Some((_, ingestion_operation)) => {
                match ingestion_operation {
                    IngestionOperation::OperationEvent(_) => {
                        // Assuming that only first message is schema update
                        assert_ne!(i, 1);
                    }
                    IngestionOperation::SchemaUpdate(_, _) => {
                        assert_eq!(i, 1)
                    }
                }
            }
        }
    }

    assert_eq!(1000, i);
}
