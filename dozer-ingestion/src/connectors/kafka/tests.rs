#[cfg(feature = "kafka_test")]
use crate::connectors::kafka::test_utils::get_iterator_and_client;

#[cfg(feature = "kafka_test")]
use dozer_types::ingestion_types::IngestionOperation;
#[cfg(feature = "kafka_test")]
use dozer_types::rust_decimal::Decimal;
#[cfg(feature = "kafka_test")]
use dozer_types::types::Operation;
#[cfg(feature = "kafka_test")]
use postgres::Client;
#[cfg(feature = "kafka_test")]
use std::fmt::Write;

#[cfg(feature = "kafka_test")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "kafka_test")]
pub struct KafkaPostgres {
    client: Client,
    table_name: String,
}

#[cfg(feature = "kafka_test")]
impl KafkaPostgres {
    pub fn insert_rows(&mut self, count: u64) {
        let mut buf = String::new();
        for i in 0..count {
            if i > 0 {
                buf.write_str(",").unwrap();
            }
            buf.write_fmt(format_args!(
                "(\'Product {}\',\'Product {} description\',{})",
                i,
                i,
                Decimal::new((i * 41) as i64, 2)
            ))
            .unwrap();
        }

        let query = format!(
            "insert into {}(name, description, weight) values {}",
            self.table_name, buf,
        );

        self.client.query(&query, &[]).unwrap();
    }

    pub fn update_rows(&mut self) {
        self.client
            .query(&format!("UPDATE {} SET weight = 5", self.table_name), &[])
            .unwrap();
    }

    pub fn delete_rows(&mut self) {
        self.client
            .query(
                &format!("DELETE FROM {} WHERE weight = 5", self.table_name),
                &[],
            )
            .unwrap();
    }
}

#[test]
#[cfg(feature = "kafka_test")]
fn connect_and_use_kafka_stream() {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let table_name = format!("products_test_{}", since_the_epoch.as_millis());
    let (iterator, client) = get_iterator_and_client("../", table_name.clone());

    let mut pg_client = KafkaPostgres { client, table_name };

    pg_client.insert_rows(10);
    pg_client.update_rows();
    pg_client.delete_rows();

    let mut i = 0;
    while i < 30 {
        let op = iterator.write().next();

        match op {
            None => {}
            Some((_, ingestion_operation)) => match ingestion_operation {
                IngestionOperation::OperationEvent(ev) => {
                    i += 1;
                    match ev.operation {
                        Operation::Insert { .. } => {
                            if i > 10 {
                                panic!("Unexpected operation");
                            }
                        }
                        Operation::Delete { .. } => {
                            if i < 21 {
                                panic!("Unexpected operation");
                            }
                        }
                        Operation::Update { .. } => {
                            if !(11..=20).contains(&i) {
                                panic!("Unexpected operation");
                            }
                        }
                    }
                }
                _ => {}
            },
        }
    }

    assert_eq!(i, 30);
}
