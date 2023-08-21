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
