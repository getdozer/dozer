use crate::ingestion::Ingestor;

use super::helper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};
use crate::errors::PostgresConnectorError::{SnapshotReadError, SyncWithSnapshotError};
use crossbeam::channel::{unbounded, Sender};

use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::connectors::TableInfo;
use crate::errors::ConnectorError::PostgresConnectorError;
use dozer_types::types::{Schema, SourceSchema};
use postgres::fallible_iterator::FallibleIterator;

use std::thread;

use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::types::Operation;

pub struct PostgresSnapshotter<'a> {
    pub tables: Vec<TableInfo>,
    pub conn_config: tokio_postgres::Config,
    pub ingestor: &'a Ingestor,
    pub connector_id: u64,
}

impl<'a> PostgresSnapshotter<'a> {
    pub fn get_tables(&self, tables: Vec<TableInfo>) -> Result<Vec<SourceSchema>, ConnectorError> {
        let helper = SchemaHelper::new(self.conn_config.clone(), None);
        helper
            .get_schemas(Some(tables))
            .map_err(PostgresConnectorError)
    }

    pub fn sync_table(
        schema: Schema,
        name: String,
        conn_config: tokio_postgres::Config,
        sender: Sender<Result<Option<Operation>, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let mut client_plain =
            connection_helper::connect(conn_config).map_err(PostgresConnectorError)?;

        let column_str: Vec<String> = schema
            .fields
            .iter()
            .map(|f| format!("\"{0}\"", f.name))
            .collect();

        let column_str = column_str.join(",");
        let query = format!("select {column_str} from {name}");
        let stmt = client_plain
            .prepare(&query)
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        let columns = stmt.columns();

        let empty_vec: Vec<String> = Vec::new();
        for msg in client_plain
            .query_raw(&stmt, empty_vec)
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?
            .iterator()
        {
            match msg {
                Ok(msg) => {
                    let evt = helper::map_row_to_operation_event(
                        name.to_string(),
                        schema
                            .identifier
                            .map_or(Err(ConnectorError::SchemaIdentifierNotFound), Ok)?,
                        &msg,
                        columns,
                    )
                    .map_err(|e| PostgresConnectorError(PostgresSchemaError(e)))?;

                    sender.send(Ok(Some(evt))).unwrap();
                }
                Err(e) => return Err(PostgresConnectorError(SyncWithSnapshotError(e.to_string()))),
            }
        }

        // After table read is finished, send None as message to inform receiver loop about end of table
        sender.send(Ok(None)).unwrap();
        Ok(())
    }

    pub fn sync_tables(&self, tables: Vec<TableInfo>) -> Result<(), ConnectorError> {
        let tables = self.get_tables(tables)?;

        let mut left_tables_count = tables.len();

        let (tx, rx) = unbounded();

        for t in tables.iter() {
            let schema = t.schema.clone();
            let name = t.name.clone();
            let conn_config = self.conn_config.clone();
            let sender = tx.clone();
            thread::spawn(move || {
                if let Err(e) = Self::sync_table(schema, name, conn_config, sender.clone()) {
                    sender.send(Err(e)).unwrap();
                }
            });
        }

        let mut idx = 0;
        loop {
            let message = rx
                .recv()
                .map_err(|_| PostgresConnectorError(SnapshotReadError))??;
            match message {
                None => {
                    left_tables_count -= 1;
                    if left_tables_count == 0 {
                        break;
                    }
                }
                Some(evt) => {
                    self.ingestor
                        .handle_message(IngestionMessage::new_op(0, idx, evt))
                        .map_err(ConnectorError::IngestorError)?;
                    idx += 1;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use postgres::NoTls;
    use serial_test::serial;

    use crate::{
        connectors::{
            postgres::connector::{PostgresConfig, PostgresConnector},
            ColumnInfo, TableInfo,
        },
        ingestion::{IngestionConfig, Ingestor},
        test_util::{get_config, run_connector_test},
    };

    use super::PostgresSnapshotter;

    #[test]
    #[serial]
    fn test_connector_snapshotter_sync_tables_successfully_1_requested_table() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);

            let table_name_1 = String::from("test_01");
            let table_name_2 = String::from("test_02");

            let column_name = String::from("column_1");

            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {0}({1} serial PRIMARY KEY);",
                        table_name_1, column_name
                    )
                    .as_str(),
                )
                .expect("User creation failed");

            client
                .simple_query(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {0}({1} serial PRIMARY KEY);",
                        table_name_2, column_name
                    )
                    .as_str(),
                )
                .expect("User creation failed");

            let connector_name = String::from("pg_connector");
            let columns = vec![ColumnInfo {
                name: column_name,
                data_type: Some(String::from("serial")),
            }];

            let tables = vec![
                TableInfo {
                    name: table_name_1.clone(),
                    table_name: table_name_1.clone(),
                    id: 0,
                    columns: Some(columns.clone()),
                },
                TableInfo {
                    name: table_name_2.clone(),
                    table_name: table_name_2.clone(),
                    id: 0,
                    columns: Some(columns.clone()),
                },
            ];

            let input_tables = vec![TableInfo {
                name: table_name_1.clone(),
                table_name: table_name_1.clone(),
                id: 0,
                columns: Some(columns.clone()),
            }];

            let postgres_config = PostgresConfig {
                name: connector_name,
                tables: Some(tables.clone()),
                config: config.clone(),
            };

            let connector = PostgresConnector::new(1, postgres_config.clone());

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut _iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                tables: Some(tables.clone()),
                conn_config: config,
                ingestor: &ingestor,
                connector_id: connector.id,
            };

            let actual = snapshotter.sync_tables(Some(input_tables));

            assert!(actual.is_ok());
            match actual {
                Err(_) | Ok(None) => panic!("Test should failed"),
                Ok(Some(result_tables)) => {
                    assert_eq!(result_tables.len(), 1);
                    assert_eq!(result_tables.get(0).unwrap().table_name, table_name_1);
                }
            }
        })
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_snapshotter_sync_tables_successfully_not_match_table() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);

            let table_name_1 = String::from("test_01");
            let table_name_2 = String::from("test_02");
            let table_name_3 = String::from("test_03");

            let column_name = String::from("column_1");

            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {0}({1} serial PRIMARY KEY);",
                        table_name_1, column_name
                    )
                    .as_str(),
                )
                .expect("User creation failed");

            client
                .simple_query(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {0}({1} serial PRIMARY KEY);",
                        table_name_2, column_name
                    )
                    .as_str(),
                )
                .expect("User creation failed");

            let connector_name = String::from("pg_connector");
            let columns = vec![ColumnInfo {
                name: column_name,
                data_type: Some(String::from("serial")),
            }];

            let tables = vec![
                TableInfo {
                    name: table_name_1.clone(),
                    table_name: table_name_1.clone(),
                    id: 0,
                    columns: Some(columns.clone()),
                },
                TableInfo {
                    name: table_name_2.clone(),
                    table_name: table_name_2.clone(),
                    id: 0,
                    columns: Some(columns.clone()),
                },
            ];

            let input_tables = vec![TableInfo {
                name: table_name_3.clone(),
                table_name: table_name_3.clone(),
                id: 0,
                columns: Some(columns.clone()),
            }];

            let postgres_config = PostgresConfig {
                name: connector_name,
                tables: Some(tables.clone()),
                config: config.clone(),
            };

            let connector = PostgresConnector::new(1, postgres_config.clone());

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut _iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                tables: Some(tables.clone()),
                conn_config: config,
                ingestor: &ingestor,
                connector_id: connector.id,
            };

            let actual = snapshotter.sync_tables(Some(input_tables));

            assert!(actual.is_err());
        })
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_snapshotter_sync_tables_successfully_table_not_exist() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);

            let table_name_1 = String::from("test_01");
            let column_name = String::from("column_1");

            let connector_name = String::from("pg_connector");
            let columns = vec![ColumnInfo {
                name: column_name,
                data_type: Some(String::from("serial")),
            }];

            let tables = vec![TableInfo {
                name: table_name_1.clone(),
                table_name: table_name_1.clone(),
                id: 0,
                columns: Some(columns.clone()),
            }];

            let input_tables = vec![TableInfo {
                name: table_name_1.clone(),
                table_name: table_name_1.clone(),
                id: 0,
                columns: Some(columns.clone()),
            }];

            let postgres_config = PostgresConfig {
                name: connector_name,
                tables: Some(tables.clone()),
                config: config.clone(),
            };

            let connector = PostgresConnector::new(1, postgres_config.clone());

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut _iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                tables: Some(tables.clone()),
                conn_config: config,
                ingestor: &ingestor,
                connector_id: connector.id,
            };

            let actual = snapshotter.sync_tables(Some(input_tables));

            assert!(actual.is_err());
        })
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_snapshotter_sync_tables_successfully_none_table_config() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);

            let table_name_1 = String::from("test_01");
            let table_name_2 = String::from("test_02");

            let column_name = String::from("column_1");

            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {0}({1} serial PRIMARY KEY);",
                        table_name_1, column_name
                    )
                    .as_str(),
                )
                .expect("User creation failed");

            client
                .simple_query(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {0}({1} serial PRIMARY KEY);",
                        table_name_2, column_name
                    )
                    .as_str(),
                )
                .expect("User creation failed");

            let connector_name = String::from("pg_connector");
            let columns = vec![ColumnInfo {
                name: column_name,
                data_type: Some(String::from("serial")),
            }];

            let tables = vec![
                TableInfo {
                    name: table_name_1.clone(),
                    table_name: table_name_1.clone(),
                    id: 0,
                    columns: Some(columns.clone()),
                },
                TableInfo {
                    name: table_name_2.clone(),
                    table_name: table_name_2.clone(),
                    id: 0,
                    columns: Some(columns.clone()),
                },
            ];

            let postgres_config = PostgresConfig {
                name: connector_name,
                tables: Some(tables.clone()),
                config: config.clone(),
            };

            let connector = PostgresConnector::new(1, postgres_config.clone());

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut _iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                tables: Some(tables.clone()),
                conn_config: config,
                ingestor: &ingestor,
                connector_id: connector.id,
            };

            let actual = snapshotter.sync_tables(None);

            assert!(actual.is_ok());
            match actual {
                Err(_) | Ok(None) => panic!("Test should failed"),
                Ok(Some(result_tables)) => {
                    assert_eq!(result_tables.len(), 0);
                }
            }
        })
    }
}
