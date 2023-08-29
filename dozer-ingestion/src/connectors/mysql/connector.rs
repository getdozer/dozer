use super::{
    binlog::{get_binlog_format, get_master_binlog_position, BinlogIngestor, BinlogPosition},
    conversion::IntoFields,
    helpers::{escape_identifier, qualify_table_name},
    schema::{ColumnDefinition, SchemaHelper, TableDefinition},
};
use crate::{
    connectors::{
        CdcType, Connector, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
    },
    errors::MySQLConnectorError,
};
use crate::{errors::ConnectorError, ingestion::Ingestor};
use dozer_types::{
    ingestion_types::IngestionMessage,
    types::{FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition},
};
use mysql_async::{prelude::Queryable, Conn, Opts, Pool};
use mysql_common::Row;
use tonic::async_trait;

#[derive(Debug)]
pub struct MySQLConnector {
    conn_url: String,
    conn_pool: Pool,
    server_id: Option<u32>,
}

pub fn mysql_connection_opts_from_url(url: &str) -> Result<Opts, MySQLConnectorError> {
    Opts::from_url(url).map_err(MySQLConnectorError::InvalidConnectionURLError)
}

impl MySQLConnector {
    pub fn new(conn_url: String, opts: Opts, server_id: Option<u32>) -> MySQLConnector {
        MySQLConnector {
            conn_url,
            conn_pool: Pool::new(opts),
            server_id,
        }
    }
}

#[async_trait]
impl Connector for MySQLConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        vec![
            ("decimal".into(), Some(FieldType::Decimal)),
            ("tinyint unsigned".into(), Some(FieldType::UInt)),
            ("tinyint".into(), Some(FieldType::Int)),
            ("smallint unsigned".into(), Some(FieldType::UInt)),
            ("smallint".into(), Some(FieldType::Int)),
            ("mediumint unsigned".into(), Some(FieldType::UInt)),
            ("mediumint".into(), Some(FieldType::Int)),
            ("int unsigned".into(), Some(FieldType::UInt)),
            ("int".into(), Some(FieldType::Int)),
            ("bigint unsigned".into(), Some(FieldType::UInt)),
            ("bigint".into(), Some(FieldType::Int)),
            ("float".into(), Some(FieldType::Float)),
            ("double".into(), Some(FieldType::Float)),
            ("timestamp".into(), Some(FieldType::Timestamp)),
            ("time".into(), Some(FieldType::Duration)),
            ("year".into(), Some(FieldType::Int)),
            ("date".into(), Some(FieldType::Date)),
            ("datetime".into(), Some(FieldType::Timestamp)),
            ("varchar".into(), Some(FieldType::Text)),
            ("varbinary".into(), Some(FieldType::Binary)),
            ("char".into(), Some(FieldType::String)),
            ("binary".into(), Some(FieldType::Binary)),
            ("tinyblob".into(), Some(FieldType::Binary)),
            ("blob".into(), Some(FieldType::Binary)),
            ("mediumblob".into(), Some(FieldType::Binary)),
            ("longblob".into(), Some(FieldType::Binary)),
            ("tinytext".into(), Some(FieldType::Text)),
            ("text".into(), Some(FieldType::Text)),
            ("mediumtext".into(), Some(FieldType::Text)),
            ("longtext".into(), Some(FieldType::Text)),
            ("point".into(), Some(FieldType::Point)),
            ("json".into(), Some(FieldType::Json)),
            ("bit".into(), Some(FieldType::Int)),
            ("enum".into(), Some(FieldType::String)),
            ("set".into(), Some(FieldType::String)),
            ("null".into(), None),
            ("linestring".into(), None),
            ("polygon".into(), None),
            ("multipoint".into(), None),
            ("multilinestring".into(), None),
            ("multipolygon".into(), None),
            ("geomcollection".into(), None),
            ("geometry".into(), None),
        ]
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        let _ = self.connect().await?;

        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        let tables = self.schema_helper().list_tables().await?;
        Ok(tables)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        let existing_tables = self.list_tables().await?;
        for table in tables {
            if !existing_tables.contains(table) {
                Err(ConnectorError::TableNotFound(qualify_table_name(
                    table.schema.as_deref(),
                    &table.name,
                )))?;
            }
        }

        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let tables_infos = self.schema_helper().list_columns(tables).await?;
        Ok(tables_infos)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        if table_infos.is_empty() {
            return Ok(Vec::new());
        }

        let table_definitions = self
            .schema_helper()
            .get_table_definitions(table_infos)
            .await?;

        let binlog_logging_format: String = get_binlog_format(&mut self.connect().await?).await?;
        let cdc_type = if binlog_logging_format == "ROW" {
            CdcType::FullChanges
        } else {
            CdcType::Nothing
        };

        let schemas = table_definitions
            .into_iter()
            .map(|TableDefinition { columns, .. }| {
                let primary_index = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, ColumnDefinition { primary_key, .. })| *primary_key)
                    .map(|(i, _)| i)
                    .collect();
                Ok(SourceSchema {
                    schema: Schema {
                        fields: columns
                            .into_iter()
                            .map(
                                |ColumnDefinition {
                                     name,
                                     typ,
                                     nullable,
                                     ..
                                 }| {
                                    FieldDefinition {
                                        name,
                                        typ,
                                        nullable,
                                        source: SourceDefinition::Dynamic,
                                    }
                                },
                            )
                            .collect(),
                        primary_index,
                    },
                    cdc_type,
                })
            })
            .collect();

        Ok(schemas)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        self.replicate(ingestor, tables).await.map_err(Into::into)
    }
}

impl MySQLConnector {
    async fn connect(&self) -> Result<Conn, MySQLConnectorError> {
        self.conn_pool
            .get_conn()
            .await
            .map_err(|err| MySQLConnectorError::ConnectionFailure(self.conn_url.clone(), err))
    }

    fn schema_helper(&self) -> SchemaHelper<'_, '_> {
        SchemaHelper::new(&self.conn_url, &self.conn_pool)
    }

    async fn replicate(
        &self,
        ingestor: &Ingestor,
        table_infos: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let mut txn = 0;

        let table_definitions = self
            .schema_helper()
            .get_table_definitions(&table_infos)
            .await?;
        let binlog_positions = self
            .replicate_tables(ingestor, &table_definitions, &mut txn)
            .await?;

        let binlog_position = self
            .sync_with_binlog(ingestor, binlog_positions, &mut txn)
            .await?;

        self.ingest_binlog(
            ingestor,
            &table_definitions,
            binlog_position,
            None,
            &mut txn,
        )
        .await?;

        Ok(())
    }

    async fn replicate_tables(
        &self,
        ingestor: &Ingestor,
        table_definitions: &[TableDefinition],
        txn: &mut u64,
    ) -> Result<Vec<(TableDefinition, BinlogPosition)>, ConnectorError> {
        let mut binlog_position_per_table = Vec::new();

        let mut conn = self.connect().await?;

        for (table_index, td) in table_definitions.iter().enumerate() {
            conn.query_drop(format!(
                "LOCK TABLES {} READ",
                qualify_table_name(Some(&td.database_name), &td.table_name)
            ))
            .await
            .map_err(MySQLConnectorError::QueryExecutionError)?;

            let row_count = {
                let mut row: Row = conn
                    .exec_first(
                        format!(
                            "SELECT COUNT(*) from {}",
                            qualify_table_name(Some(&td.database_name), &td.table_name)
                        ),
                        (),
                    )
                    .await
                    .map_err(MySQLConnectorError::QueryExecutionError)?
                    .unwrap();
                let count: u64 = row.take(0).unwrap();
                count
            };

            if row_count == 0 {
                conn.query_drop("UNLOCK TABLES")
                    .await
                    .map_err(MySQLConnectorError::QueryExecutionError)?;
                continue;
            }

            let mut seq_no = 0u64;

            ingestor
                .handle_message(IngestionMessage::new_snapshotting_started(*txn, seq_no))
                .map_err(ConnectorError::IngestorError)?;

            let mut rows = conn
                .exec_iter(
                    format!(
                        "SELECT {} from {}",
                        td.columns
                            .iter()
                            .map(|ColumnDefinition { name, .. }| escape_identifier(name))
                            .collect::<Vec<String>>()
                            .join(", "),
                        qualify_table_name(Some(&td.database_name), &td.table_name)
                    ),
                    (),
                )
                .await
                .map_err(MySQLConnectorError::QueryExecutionError)?;

            let field_types: Vec<FieldType> = td
                .columns
                .iter()
                .map(|ColumnDefinition { typ, .. }| *typ)
                .collect();

            while let Some(row) = rows
                .next()
                .await
                .map_err(MySQLConnectorError::QueryResultError)?
            {
                let op: Operation = Operation::Insert {
                    new: Record::new(row.into_fields(&field_types)?),
                };

                seq_no += 1;
                ingestor
                    .handle_message(IngestionMessage::new_op(*txn, seq_no, table_index, op))
                    .map_err(ConnectorError::IngestorError)?;
            }

            let binlog_position = get_master_binlog_position(&mut conn).await?;

            conn.query_drop("UNLOCK TABLES")
                .await
                .map_err(MySQLConnectorError::QueryExecutionError)?;

            seq_no += 1;
            ingestor
                .handle_message(IngestionMessage::new_snapshotting_done(*txn, seq_no))
                .map_err(ConnectorError::IngestorError)?;

            binlog_position_per_table.push((td.clone(), binlog_position));

            *txn += 1;
        }

        Ok(binlog_position_per_table)
    }

    async fn sync_with_binlog(
        &self,
        ingestor: &Ingestor,
        binlog_positions: Vec<(TableDefinition, BinlogPosition)>,
        txn: &mut u64,
    ) -> Result<BinlogPosition, ConnectorError> {
        assert!(!binlog_positions.is_empty());

        let position = {
            let mut last_position = None;
            let mut synced_tables = Vec::new();

            for (table, position) in binlog_positions.into_iter() {
                synced_tables.push(table);

                if let Some(start_position) = last_position {
                    let end_position = position.clone();

                    self.ingest_binlog(
                        ingestor,
                        &synced_tables,
                        start_position,
                        Some(end_position),
                        txn,
                    )
                    .await?;
                }

                last_position = Some(position);
            }

            last_position.unwrap()
        };

        Ok(position)
    }

    async fn ingest_binlog(
        &self,
        ingestor: &Ingestor,
        tables: &[TableDefinition],
        start_position: BinlogPosition,
        stop_position: Option<BinlogPosition>,
        txn: &mut u64,
    ) -> Result<(), ConnectorError> {
        let server_id = self.server_id.unwrap_or(0xd07e5);

        let mut binlog_ingestor = BinlogIngestor::new(
            ingestor,
            tables,
            start_position,
            stop_position,
            server_id,
            txn,
            (&self.conn_pool, &self.conn_url),
        );

        binlog_ingestor.ingest().await
    }
}

#[cfg(test)]
mod tests {
    use super::MySQLConnector;
    use crate::{
        connectors::{
            mysql::tests::{
                create_test_table, mariadb_test_config, mysql_test_config, MockIngestionStream,
                TestConfig,
            },
            CdcType, Connector, SourceSchema, TableIdentifier,
        },
        ingestion::Ingestor,
    };
    use dozer_types::{
        ingestion_types::IngestionMessage,
        json_types::JsonValue,
        types::{
            Field, FieldDefinition, FieldType, Operation::*, Record, Schema, SourceDefinition,
        },
    };
    use mysql_async::prelude::Queryable;
    use serial_test::serial;
    use std::{
        sync::{mpsc::Receiver, Arc},
        time::Duration,
    };

    struct TestCtx {
        pub connector: MySQLConnector,
        pub ingestor: Ingestor,
        pub message_channel: Receiver<IngestionMessage>,
    }

    impl TestCtx {
        async fn setup(config: &TestConfig) -> Self {
            let url = config.url.clone();
            let opts = config.opts.clone();

            let (sender, receiver) = std::sync::mpsc::channel();

            let ingestion_stream = MockIngestionStream::new(sender);
            let ingestor = Ingestor {
                sender: Arc::new(Box::new(ingestion_stream)),
            };
            let connector = MySQLConnector::new(url, opts, Some(10));

            Self {
                connector,
                ingestor,
                message_channel: receiver,
            }
        }
    }

    fn check_ingestion_messages(
        message_channel: &Receiver<IngestionMessage>,
        expected_ingestion_messages: Vec<IngestionMessage>,
    ) {
        let actual_ingestion_messages =
            message_channel.take_timeout(expected_ingestion_messages.len(), Duration::from_secs(5));

        for (i, (actual, expected)) in std::iter::zip(
            actual_ingestion_messages.iter(),
            expected_ingestion_messages.iter(),
        )
        .enumerate()
        {
            assert_eq!(
                expected, actual,
                "The {i}th message didn't match. Expected {expected:?}; Found {actual:?}\nThe actual message queue is {actual_ingestion_messages:?}"
            );
        }
    }

    trait TakeTimeout {
        fn take_timeout(&self, n: usize, timeout: Duration) -> Vec<IngestionMessage>;
    }

    impl TakeTimeout for Receiver<IngestionMessage> {
        fn take_timeout(&self, n: usize, timeout: Duration) -> Vec<IngestionMessage> {
            let mut vec = Vec::new();
            for _ in 0..n {
                let msg = self.recv_timeout(timeout).unwrap();
                vec.push(msg);
            }
            vec
        }
    }

    async fn test_connector_simple_table_replication(config: TestConfig) {
        // setup
        let TestCtx {
            connector,
            ingestor,
            message_channel,
            ..
        } = TestCtx::setup(&config).await;

        let table_info = create_test_table("test1", &config).await;
        let table_definitions = connector
            .schema_helper()
            .get_table_definitions(&[table_info])
            .await
            .unwrap();

        let mut conn = connector.conn_pool.get_conn().await.unwrap();

        // test
        conn.exec_drop(
            "
                REPLACE INTO test1
                VALUES
                    (1, 'a', 1.0),
                    (2, 'b', 2.0),
                    (3, 'c', 3.0)
                ",
            (),
        )
        .await
        .unwrap();

        let result = connector
            .replicate_tables(&ingestor, &table_definitions, &mut 0)
            .await;
        assert!(result.is_ok(), "unexpected error: {result:?}");

        let expected_ingestion_messages = vec![
            IngestionMessage::new_snapshotting_started(0, 0),
            IngestionMessage::new_op(
                0,
                1,
                0,
                Insert {
                    new: Record::new(vec![
                        Field::Int(1),
                        Field::Text("a".into()),
                        Field::Float(1.0.into()),
                    ]),
                },
            ),
            IngestionMessage::new_op(
                0,
                2,
                0,
                Insert {
                    new: Record::new(vec![
                        Field::Int(2),
                        Field::Text("b".into()),
                        Field::Float(2.0.into()),
                    ]),
                },
            ),
            IngestionMessage::new_op(
                0,
                3,
                0,
                Insert {
                    new: Record::new(vec![
                        Field::Int(3),
                        Field::Text("c".into()),
                        Field::Float(3.0.into()),
                    ]),
                },
            ),
            IngestionMessage::new_snapshotting_done(0, 4),
        ];

        check_ingestion_messages(&message_channel, expected_ingestion_messages);
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_simple_table_replication_mysql() {
        test_connector_simple_table_replication(mysql_test_config()).await;
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_simple_table_replication_mariadb() {
        test_connector_simple_table_replication(mariadb_test_config()).await;
    }

    async fn test_connector_cdc(config: TestConfig) {
        // setup
        let TestCtx {
            connector,
            ingestor,
            message_channel,
            ..
        } = TestCtx::setup(&config).await;

        let mut table_infos = Vec::new();
        table_infos.push(create_test_table("test3", &config).await);
        table_infos.push(create_test_table("test2", &config).await);

        let mut conn = connector.conn_pool.get_conn().await.unwrap();

        conn.exec_drop("DELETE FROM test2", ()).await.unwrap();
        conn.exec_drop("DELETE FROM test3", ()).await.unwrap();

        let _handle = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async move {
                    let _ = connector.replicate(&ingestor, table_infos).await;
                });
        });

        // test insert
        conn.exec_drop("REPLACE INTO test3 VALUES (4, 4.0)", ())
            .await
            .unwrap();

        conn.exec_drop("REPLACE INTO test2 VALUES (1, 'true')", ())
            .await
            .unwrap();

        let expected_ingestion_messages = vec![
            IngestionMessage::new_snapshotting_started(0, 0),
            IngestionMessage::new_op(
                0,
                1,
                0,
                Insert {
                    new: Record::new(vec![Field::Int(4), Field::Float(4.0.into())]),
                },
            ),
            IngestionMessage::new_snapshotting_done(0, 2),
            IngestionMessage::new_snapshotting_started(1, 0),
            IngestionMessage::new_op(
                1,
                1,
                1,
                Insert {
                    new: Record::new(vec![Field::Int(1), Field::Json(JsonValue::Bool(true))]),
                },
            ),
            IngestionMessage::new_snapshotting_done(1, 2),
        ];

        check_ingestion_messages(&message_channel, expected_ingestion_messages);

        // test update
        conn.exec_drop("UPDATE test3 SET b = 5.0 WHERE a = 4", ())
            .await
            .unwrap();

        let expected_ingestion_messages = vec![
            IngestionMessage::new_snapshotting_started(2, 0),
            IngestionMessage::new_op(
                2,
                1,
                0,
                Update {
                    old: Record::new(vec![Field::Int(4), Field::Float(4.0.into())]),
                    new: Record::new(vec![Field::Int(4), Field::Float(5.0.into())]),
                },
            ),
            IngestionMessage::new_snapshotting_done(2, 2),
        ];

        check_ingestion_messages(&message_channel, expected_ingestion_messages);

        // test delete
        conn.exec_drop("DELETE FROM test3 WHERE a = 4", ())
            .await
            .unwrap();

        let expected_ingestion_messages = vec![
            IngestionMessage::new_snapshotting_started(3, 0),
            IngestionMessage::new_op(
                3,
                1,
                0,
                Delete {
                    old: Record::new(vec![Field::Int(4), Field::Float(5.0.into())]),
                },
            ),
            IngestionMessage::new_snapshotting_done(3, 2),
        ];

        check_ingestion_messages(&message_channel, expected_ingestion_messages);
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_cdc_mysql() {
        test_connector_cdc(mysql_test_config()).await;
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_cdc_mariadb() {
        test_connector_cdc(mariadb_test_config()).await;
    }

    async fn test_connector_schemas(config: TestConfig) {
        // setup
        let TestCtx { connector, .. } = TestCtx::setup(&config).await;

        let mut expected_table_infos = Vec::new();
        expected_table_infos.push(create_test_table("test1", &config).await);
        expected_table_infos.push(create_test_table("test2", &config).await);

        let expected_table_identifiers = vec![
            TableIdentifier {
                schema: Some("test".into()),
                name: "test1".into(),
            },
            TableIdentifier {
                schema: Some("test".into()),
                name: "test2".into(),
            },
        ];

        // test list_tables
        let result = connector.list_tables().await;
        assert!(result.is_ok(), "unexpected error: {result:?}");
        let tables = result.unwrap();
        for table_identifier in expected_table_identifiers.iter() {
            assert!(
                tables.contains(table_identifier),
                "missing {table_identifier:?} from list {tables:?}"
            );
        }

        // test list_columns
        let result = connector.list_columns(expected_table_identifiers).await;
        assert!(result.is_ok(), "unexpected error: {result:?}");
        let table_infos = result.unwrap();
        for (i, (expected, actual)) in
            std::iter::zip(expected_table_infos.iter(), table_infos.iter()).enumerate()
        {
            assert_eq!(
                expected, actual,
                "The {i}th table doesn't match! Expected {expected:?}; Found {actual:?}"
            );
        }

        // test get_schemas
        let result = connector.get_schemas(&table_infos).await;
        assert!(result.is_ok(), "unexpected error: {result:?}");
        let source_schema_results = result.unwrap();

        let expected_source_schema_results = [
            SourceSchema {
                schema: Schema {
                    fields: vec![
                        FieldDefinition {
                            name: "c1".into(),
                            typ: FieldType::Int,
                            nullable: false,
                            source: SourceDefinition::Dynamic,
                        },
                        FieldDefinition {
                            name: "c2".into(),
                            typ: FieldType::Text,
                            nullable: true,
                            source: SourceDefinition::Dynamic,
                        },
                        FieldDefinition {
                            name: "c3".into(),
                            typ: FieldType::Float,
                            nullable: true,
                            source: SourceDefinition::Dynamic,
                        },
                    ],
                    primary_index: vec![0],
                },
                cdc_type: CdcType::FullChanges,
            },
            SourceSchema {
                schema: Schema {
                    fields: vec![
                        FieldDefinition {
                            name: "id".into(),
                            typ: FieldType::Int,
                            nullable: false,
                            source: SourceDefinition::Dynamic,
                        },
                        FieldDefinition {
                            name: "value".into(),
                            typ: FieldType::Json,
                            nullable: true,
                            source: SourceDefinition::Dynamic,
                        },
                    ],
                    primary_index: vec![0],
                },
                cdc_type: CdcType::FullChanges,
            },
        ];

        for (i, (expected, actual)) in std::iter::zip(
            expected_source_schema_results.iter(),
            source_schema_results.iter(),
        )
        .enumerate()
        {
            assert!(actual.is_ok(), "unexpected error: {actual:?}");
            let actual = actual.as_ref().unwrap();
            assert_eq!(
                expected, actual,
                "The {i}th table doesn't match! Expected {expected:?}; Found {actual:?}"
            );
        }
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_schemas_mysql() {
        test_connector_schemas(mysql_test_config()).await;
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_schemas_mariadb() {
        test_connector_schemas(mariadb_test_config()).await;
    }
}
