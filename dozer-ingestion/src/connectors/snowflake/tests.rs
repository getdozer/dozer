use std::thread;
use dozer_types::ingestion_types::IngestionOperation;
use crate::connectors::snowflake::test_utils::remove_streams;
use crate::connectors::{get_connector, TableInfo};
use crate::errors::ConnectorError;
use crate::ingestion::{IngestionConfig, Ingestor};
use crate::ingestion::test_utils::load_config;

#[cfg(feature = "snowflake")]
#[test]
fn connect_and_read_from_snowflake_stream() {
    let source = load_config("../dozer-config.test.snowflake.yaml".to_string()).unwrap();

    remove_streams(source.connection.clone(), &source.table_name).unwrap();

    let config = IngestionConfig::default();

    let (ingestor, iterator) = Ingestor::initialize_channel(config);

    thread::spawn(|| -> Result<(), ConnectorError> {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: source.table_name,
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(source.connection).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        match connector.start() {
            Ok(_) => {}
            Err(_) => {}
        }

        Ok(())
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