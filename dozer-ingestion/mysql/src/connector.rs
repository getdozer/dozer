use crate::MySQLConnectorError;

use super::{
    binlog::{get_binlog_format, get_master_binlog_position, BinlogIngestor, BinlogPosition},
    connection::Conn,
    conversion::IntoFields,
    helpers::{escape_identifier, qualify_table_name},
    schema::{ColumnDefinition, SchemaHelper, TableDefinition},
};
use crate::MySQLConnectorError::BinlogQueryError;
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError,
        log::info,
        models::ingestion_types::{IngestionMessage, TransactionInfo},
        node::{OpIdentifier, SourceState},
        types::{FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition},
    },
    utils::TableNotFound,
    CdcType, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};
use mysql_async::{Opts, Pool};
use mysql_common::Row;
use rand::Rng;

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
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
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

    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        let _ = self.connect().await?;

        Ok(())
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let tables = self.schema_helper().list_tables().await?;
        Ok(tables)
    }

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let existing_tables = self.list_tables().await?;
        for table in tables {
            if !existing_tables.contains(table) {
                Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                })?;
            }
        }

        Ok(())
    }

    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let tables_infos = self.schema_helper().list_columns(tables).await?;
        Ok(tables_infos)
    }

    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
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
                                        description: None,
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

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        self.replicate(ingestor, tables, last_checkpoint.op_id())
            .await
            .map_err(Into::into)
    }
}

impl MySQLConnector {
    async fn connect(&self) -> Result<Conn, MySQLConnectorError> {
        Conn::new(self.conn_pool.clone())
            .await
            .map_err(|err| MySQLConnectorError::ConnectionFailure(self.conn_url.clone(), err))
    }

    fn schema_helper(&self) -> SchemaHelper<'_> {
        SchemaHelper::new(&self.conn_url, &self.conn_pool)
    }

    async fn replicate(
        &self,
        ingestor: &Ingestor,
        table_infos: Vec<TableInfo>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), MySQLConnectorError> {
        let mut table_definitions = self
            .schema_helper()
            .get_table_definitions(
                table_infos
                    .iter()
                    .map(|table| TableInfo {
                        schema: table.schema.clone(),
                        name: table.name.clone(),
                        column_names: table.column_names.clone(),
                    })
                    .collect::<Vec<TableInfo>>()
                    .as_slice(),
            )
            .await?;

        let binlog_position = last_checkpoint
            .map(crate::binlog::BinlogPosition::try_from)
            .transpose()?;

        let binlog_positions = self
            .replicate_tables(ingestor, &table_definitions, binlog_position)
            .await?;

        let binlog_position = self.sync_with_binlog(ingestor, binlog_positions).await?;

        let prefix = self.get_prefix(binlog_position.binlog_id).await?;

        info!("Ingestion starting at {:?}", binlog_position);
        self.ingest_binlog(
            ingestor,
            &mut table_definitions,
            binlog_position,
            None,
            prefix,
        )
        .await?;

        Ok(())
    }

    async fn get_prefix(&self, suffix: u64) -> Result<String, MySQLConnectorError> {
        let suffix_formatted = format!("{:0>6}", suffix);
        let mut conn = self.connect().await?;
        let mut rows = conn.exec_iter("SHOW BINARY LOGS".to_string(), vec![]);

        let mut prefix = None;
        while let Some(result) = rows.next().await {
            let binlog_id = result
                .map_err(MySQLConnectorError::QueryResultError)?
                .get::<String, _>(0)
                .ok_or(BinlogQueryError)?;

            let row_binlog_suffix = &binlog_id[binlog_id.len() - 6..];

            if row_binlog_suffix == suffix_formatted {
                if prefix.is_some() {
                    return Err(MySQLConnectorError::MultipleBinlogsWithSameSuffix);
                }

                prefix = Some(binlog_id[..(binlog_id.len() - 7)].to_string());
            }
        }

        if let Some(prefix) = prefix {
            Ok(prefix)
        } else {
            Err(MySQLConnectorError::BinlogNotFound)
        }
    }

    async fn replicate_tables(
        &self,
        ingestor: &Ingestor,
        table_definitions: &[TableDefinition],
        binlog_position: Option<BinlogPosition>,
    ) -> Result<Vec<(TableDefinition, BinlogPosition)>, MySQLConnectorError> {
        let mut binlog_position_per_table = Vec::new();

        let mut conn = self.connect().await?;

        let mut snapshot_started = false;
        for (table_index, td) in table_definitions.iter().enumerate() {
            let position = match &binlog_position {
                Some(position) => position.clone(),
                _ => {
                    if !snapshot_started {
                        if ingestor
                            .handle_message(IngestionMessage::TransactionInfo(
                                TransactionInfo::SnapshottingStarted,
                            ))
                            .await
                            .is_err()
                        {
                            // If receiving end is closed, we should stop the replication
                            break;
                        }
                        snapshot_started = true;
                    }

                    conn.query_drop(&format!(
                        "LOCK TABLES {} READ",
                        qualify_table_name(Some(&td.database_name), &td.table_name)
                    ))
                    .await
                    .map_err(MySQLConnectorError::QueryExecutionError)?;

                    let row_count = {
                        let mut row: Row = conn
                            .exec_first(
                                &format!(
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

                    let mut rows = conn.exec_iter(
                        format!(
                            "SELECT {} from {}",
                            td.columns
                                .iter()
                                .map(|ColumnDefinition { name, .. }| escape_identifier(name))
                                .collect::<Vec<String>>()
                                .join(", "),
                            qualify_table_name(Some(&td.database_name), &td.table_name)
                        ),
                        vec![],
                    );

                    let field_types: Vec<FieldType> = td
                        .columns
                        .iter()
                        .map(|ColumnDefinition { typ, .. }| *typ)
                        .collect();

                    while let Some(result) = rows.next().await {
                        let row = result.map_err(MySQLConnectorError::QueryResultError)?;
                        let op: Operation = Operation::Insert {
                            new: Record::new(row.into_fields(&field_types)?),
                        };

                        if ingestor
                            .handle_message(IngestionMessage::OperationEvent {
                                table_index,
                                op,
                                id: None,
                            })
                            .await
                            .is_err()
                        {
                            // If receiving end is closed, we should stop the replication
                            break;
                        }
                    }

                    let (_prefix, binlog_position) = get_master_binlog_position(&mut conn).await?;

                    conn.query_drop("UNLOCK TABLES")
                        .await
                        .map_err(MySQLConnectorError::QueryExecutionError)?;

                    binlog_position
                }
            };

            binlog_position_per_table.push((td.clone(), position));
        }

        if snapshot_started
            && ingestor
                .handle_message(IngestionMessage::TransactionInfo(
                    TransactionInfo::SnapshottingDone { id: None },
                ))
                .await
                .is_err()
        {
            return Err(MySQLConnectorError::SnapshotIngestionMessageError);
        }

        Ok(binlog_position_per_table)
    }

    async fn sync_with_binlog(
        &self,
        ingestor: &Ingestor,
        binlog_positions: Vec<(TableDefinition, BinlogPosition)>,
    ) -> Result<BinlogPosition, MySQLConnectorError> {
        assert!(!binlog_positions.is_empty());

        let position = {
            let mut last_position: Option<BinlogPosition> = None;
            let mut synced_tables = Vec::new();

            for (table, position) in binlog_positions.into_iter() {
                synced_tables.push(table);

                if let Some(start_position) = last_position {
                    let end_position = position.clone();

                    let prefix = self.get_prefix(start_position.binlog_id).await?;

                    self.ingest_binlog(
                        ingestor,
                        &mut synced_tables,
                        start_position,
                        Some(end_position),
                        prefix,
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
        tables: &mut [TableDefinition],
        start_position: BinlogPosition,
        stop_position: Option<BinlogPosition>,
        binlog_prefix: String,
    ) -> Result<(), MySQLConnectorError> {
        let server_id = self.server_id.unwrap_or_else(|| rand::thread_rng().gen());

        let mut binlog_ingestor = BinlogIngestor::new(
            ingestor,
            start_position,
            stop_position,
            server_id,
            (&self.conn_pool, &self.conn_url),
            binlog_prefix,
        );

        binlog_ingestor.ingest(tables, self.schema_helper()).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        connection::Conn,
        tests::{create_test_table, mariadb_test_config, mysql_test_config, TestConfig},
    };

    use super::MySQLConnector;
    use dozer_ingestion_connector::{
        dozer_types::{
            models::ingestion_types::{IngestionMessage, TransactionInfo},
            types::{
                Field, FieldDefinition, FieldType, Operation::*, Record, Schema, SourceDefinition,
            },
        },
        tokio, CdcType, Connector, IngestionIterator, Ingestor, SourceSchema, TableIdentifier,
    };
    use serial_test::serial;
    use std::time::Duration;

    struct TestCtx {
        pub connector: MySQLConnector,
        pub ingestor: Ingestor,
        pub iterator: IngestionIterator,
    }

    impl TestCtx {
        async fn setup(config: &TestConfig) -> Self {
            let url = config.url.clone();
            let opts = config.opts.clone();

            let (ingestor, iterator) = Ingestor::initialize_channel(Default::default());
            let connector = MySQLConnector::new(url, opts, Some(10));

            Self {
                connector,
                ingestor,
                iterator,
            }
        }
    }

    async fn check_ingestion_messages(
        iterator: &mut IngestionIterator,
        expected_ingestion_messages: Vec<IngestionMessage>,
    ) {
        let mut actual_ingestion_messages = take_timeout(
            iterator,
            expected_ingestion_messages.len(),
            Duration::from_secs(5),
        )
        .await;

        // We are not checking state.
        for actual in actual_ingestion_messages.iter_mut() {
            match actual {
                IngestionMessage::OperationEvent { id, .. } => {
                    *id = None;
                }
                IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                    id,
                    source_time: None,
                }) => {
                    *id = None;
                }
                _ => {}
            }
        }

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

    async fn take_timeout(
        iterator: &mut IngestionIterator,
        n: usize,
        timeout: Duration,
    ) -> Vec<IngestionMessage> {
        let mut vec = Vec::new();
        for _ in 0..n {
            let msg = iterator.next_timeout(timeout).await.unwrap();
            vec.push(msg);
        }
        vec
    }

    async fn test_connector_simple_table_replication(config: TestConfig) {
        // setup
        let TestCtx {
            connector,
            ingestor,
            mut iterator,
            ..
        } = TestCtx::setup(&config).await;

        let table_info = create_test_table("test1", &config).await;
        let table_definitions = connector
            .schema_helper()
            .get_table_definitions(&[table_info])
            .await
            .unwrap();

        let mut conn = Conn::new(connector.conn_pool.clone()).await.unwrap();

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
            .replicate_tables(&ingestor, &table_definitions, None)
            .await;
        assert!(result.is_ok(), "unexpected error: {result:?}");

        let expected_ingestion_messages = vec![
            IngestionMessage::TransactionInfo(TransactionInfo::SnapshottingStarted),
            IngestionMessage::OperationEvent {
                table_index: 0,
                op: Insert {
                    new: Record::new(vec![
                        Field::Int(1),
                        Field::Text("a".into()),
                        Field::Float(1.0.into()),
                    ]),
                },
                id: None,
            },
            IngestionMessage::OperationEvent {
                table_index: 0,
                op: Insert {
                    new: Record::new(vec![
                        Field::Int(2),
                        Field::Text("b".into()),
                        Field::Float(2.0.into()),
                    ]),
                },
                id: None,
            },
            IngestionMessage::OperationEvent {
                table_index: 0,
                op: Insert {
                    new: Record::new(vec![
                        Field::Int(3),
                        Field::Text("c".into()),
                        Field::Float(3.0.into()),
                    ]),
                },
                id: None,
            },
            IngestionMessage::TransactionInfo(TransactionInfo::SnapshottingDone { id: None }),
        ];

        check_ingestion_messages(&mut iterator, expected_ingestion_messages).await;
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
            mut iterator,
            ..
        } = TestCtx::setup(&config).await;

        let mut table_infos = Vec::new();
        table_infos.push(create_test_table("test3", &config).await);
        table_infos.push(create_test_table("test2", &config).await);

        let mut conn = Conn::new(connector.conn_pool.clone()).await.unwrap();

        conn.exec_drop("DELETE FROM test2", ()).await.unwrap();
        conn.exec_drop("DELETE FROM test3", ()).await.unwrap();

        let _handle = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async move {
                    let _ = connector.replicate(&ingestor, table_infos, None).await;
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
            IngestionMessage::TransactionInfo(TransactionInfo::SnapshottingStarted),
            IngestionMessage::OperationEvent {
                table_index: 0,
                op: Insert {
                    new: Record::new(vec![Field::Int(4), Field::Float(4.0.into())]),
                },
                id: None,
            },
            IngestionMessage::OperationEvent {
                table_index: 1,
                op: Insert {
                    new: Record::new(vec![Field::Int(1), Field::Json(true.into())]),
                },
                id: None,
            },
            IngestionMessage::TransactionInfo(TransactionInfo::SnapshottingDone { id: None }),
        ];

        check_ingestion_messages(&mut iterator, expected_ingestion_messages).await;

        // test update
        conn.exec_drop("UPDATE test3 SET b = 5.0 WHERE a = 4", ())
            .await
            .unwrap();

        let expected_ingestion_messages = vec![
            IngestionMessage::OperationEvent {
                table_index: 0,
                op: Update {
                    old: Record::new(vec![Field::Int(4), Field::Float(4.0.into())]),
                    new: Record::new(vec![Field::Int(4), Field::Float(5.0.into())]),
                },
                id: None,
            },
            IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                id: None,
                source_time: None,
            }),
        ];

        check_ingestion_messages(&mut iterator, expected_ingestion_messages).await;

        // test delete
        conn.exec_drop("DELETE FROM test3 WHERE a = 4", ())
            .await
            .unwrap();

        let expected_ingestion_messages = vec![
            IngestionMessage::OperationEvent {
                table_index: 0,
                op: Delete {
                    old: Record::new(vec![Field::Int(4), Field::Float(5.0.into())]),
                },
                id: None,
            },
            IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                id: None,
                source_time: None,
            }),
        ];

        check_ingestion_messages(&mut iterator, expected_ingestion_messages).await;
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
        let TestCtx { mut connector, .. } = TestCtx::setup(&config).await;

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
                            description: None,
                        },
                        FieldDefinition {
                            name: "c2".into(),
                            typ: FieldType::Text,
                            nullable: true,
                            source: SourceDefinition::Dynamic,
                            description: None,
                        },
                        FieldDefinition {
                            name: "c3".into(),
                            typ: FieldType::Float,
                            nullable: true,
                            source: SourceDefinition::Dynamic,
                            description: None,
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
                            description: None,
                        },
                        FieldDefinition {
                            name: "value".into(),
                            typ: FieldType::Json,
                            nullable: true,
                            source: SourceDefinition::Dynamic,
                            description: None,
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
