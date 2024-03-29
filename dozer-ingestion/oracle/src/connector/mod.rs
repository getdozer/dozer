use std::{
    collections::{HashMap, HashSet},
    num::ParseFloatError,
    sync::Arc,
};

use dozer_ingestion_connector::{
    dozer_types::{
        chrono,
        epoch::SourceTime,
        log::{debug, error},
        models::ingestion_types::{
            IngestionMessage, LogMinerConfig, OracleReplicator, TransactionInfo,
        },
        node::OpIdentifier,
        rust_decimal, thiserror,
        types::{Operation, Schema},
    },
    Ingestor, SourceSchema, TableIdentifier, TableInfo,
};
use oracle::{
    sql_type::{Collection, ObjectType, OracleType, ToSql},
    Connection,
};

#[derive(Debug, Clone)]
pub struct Connector {
    connection_name: String,
    connection: Arc<Connection>,
    username: String,
    batch_size: usize,
    replicator: OracleReplicator,
}

type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error, Clone)]
pub(crate) enum ParseDateError {
    #[error("Invalid date format: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("Invalid oracle format")]
    Oracle,
}

#[derive(Debug, thiserror::Error, Clone)]
pub(crate) enum Error {
    #[error("oracle error: {0:?}")]
    Oracle(Arc<oracle::Error>),
    #[error("pdb not found: {0}")]
    PdbNotFound(String),
    #[error("table not found: {0:?}")]
    TableNotFound(TableIdentifier),
    #[error("data type: {0}")]
    DataType(#[from] mapping::DataTypeError),
    #[error("column {schema:?}.{table_name}.{column_name} not found")]
    ColumnNotFound {
        schema: Option<String>,
        table_name: String,
        column_name: String,
    },
    #[error("column count mismatch: expected {expected}, actual {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("cannot convert Oracle number to decimal: {0}. Number: {1}")]
    NumberToDecimal(rust_decimal::Error, String),
    #[error("insert failed to match: {0}")]
    InsertFailedToMatch(String),
    #[error("delete failed to match: {0}")]
    DeleteFailedToMatch(String),
    #[error("update failed to match: {0}")]
    UpdateFailedToMatch(String),
    #[error("null value for non-nullable field {0}")]
    NullValue(String),
    #[error("cannot parse float: {0}")]
    ParseFloat(#[from] ParseFloatError),
    #[error("cannot parse date time from {1}: {0}")]
    ParseDateTime(ParseDateError, String),
    #[error("got error when parsing uint {0}")]
    ParseUIntFailed(String),
    #[error("got error when parsing int {0}")]
    ParseIntFailed(String),
    #[error("No logs found for startscn {0}")]
    NoLogsFound(u64),
}

impl From<oracle::Error> for Error {
    fn from(value: oracle::Error) -> Self {
        Self::Oracle(Arc::new(value))
    }
}

/// `oracle`'s `ToSql` implementation for `&str` uses `NVARCHAR2` type, which Oracle expects to be UTF16 encoded by default.
/// Here we use `VARCHAR2` type instead, which Oracle expects to be UTF8 encoded by default.
struct OracleString<'a>(&'a str);
impl ToSql for OracleString<'_> {
    fn oratype(&self, _conn: &Connection) -> oracle::Result<OracleType> {
        Ok(OracleType::Varchar2(self.0.len() as u32))
    }

    fn to_sql(&self, val: &mut oracle::SqlValue) -> oracle::Result<()> {
        val.set(&self.0)?;
        Ok(())
    }
}

pub type Scn = u64;

impl Connector {
    pub fn new(
        connection_name: String,
        username: String,
        password: &str,
        connect_string: &str,
        batch_size: usize,
        replicator: OracleReplicator,
    ) -> Result<Self> {
        let connection = Connection::connect(&username, password, connect_string)?;

        let connector = Self {
            connection_name,
            connection: Arc::new(connection),
            username,
            batch_size,
            replicator,
        };
        Ok(connector)
    }

    pub fn get_con_id(&mut self, pdb: &str) -> Result<u32> {
        let sql = "SELECT CON_NAME_TO_ID(:1) FROM DUAL";
        let con_id = self
            .connection
            .query_row_as::<Option<u32>>(sql, &[&OracleString(pdb)])?
            .ok_or_else(|| Error::PdbNotFound(pdb.to_string()));
        self.connection.commit()?;
        con_id
    }

    pub fn list_tables(&mut self, schemas: &[String]) -> Result<Vec<TableIdentifier>> {
        let rows = if schemas.is_empty() {
            let sql = "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
            debug!("{}", sql);
            self.connection.query_as::<(String, String)>(sql, &[])?
        } else {
            let sql = "
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER IN (SELECT COLUMN_VALUE FROM TABLE(:2))
            ";
            let owners = string_collection(&self.connection, schemas)?;
            debug!("{}, {}", sql, owners);
            self.connection
                .query_as::<(String, String)>(sql, &[&owners])?
        };

        let tables = rows
            .map(|row| {
                row.map(|(owner, table_name)| TableIdentifier {
                    schema: Some(owner),
                    name: table_name,
                })
                .map_err(Into::into)
            })
            .collect();
        self.connection.commit()?;
        tables
    }

    pub fn list_columns(&mut self, tables: Vec<TableIdentifier>) -> Result<Vec<TableInfo>> {
        // List all tables and columns.
        let schemas = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<HashSet<_>>();
        let table_columns =
            listing::TableColumn::list(&self.connection, &schemas.into_iter().collect::<Vec<_>>())?;
        let mut table_to_columns = HashMap::<(String, String), Vec<String>>::new();
        for table_column in table_columns {
            let table_pair = (table_column.owner, table_column.table_name);
            table_to_columns
                .entry(table_pair)
                .or_default()
                .push(table_column.column_name);
        }

        // Collect columns for requested tables.
        let mut result = vec![];
        for table in tables {
            let schema = table
                .schema
                .clone()
                .unwrap_or_else(|| self.username.clone());
            let table_pair = (schema, table.name.clone());
            let column_names = table_to_columns
                .remove(&table_pair)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            result.push(TableInfo {
                schema: table.schema,
                name: table.name,
                column_names,
            });
        }
        self.connection.commit()?;
        Ok(result)
    }

    pub fn get_schemas<'a>(
        &mut self,
        table_infos: impl IntoIterator<Item = &'a TableInfo>,
    ) -> Result<Vec<Result<SourceSchema>>> {
        let table_infos: Vec<_> = table_infos.into_iter().collect();
        // Collect all tables and columns.
        let table_names = table_infos
            .iter()
            .map(|table| table.name.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let table_columns = listing::TableColumn::list(&self.connection, &table_names)?;
        let constraint_columns =
            listing::ConstraintColumn::list(&self.connection, &table_names).unwrap();
        let constraints = listing::Constraint::list(&self.connection, &table_names).unwrap();
        let table_columns =
            join::join_columns_constraints(table_columns, constraint_columns, constraints);

        // Map all the columns.
        let mut table_columns = mapping::map_tables(table_columns);

        // Decide `SourceSchemaResult` for each `table_info`
        let mut result = vec![];
        for table_info in table_infos {
            let schema = table_info
                .schema
                .clone()
                .unwrap_or_else(|| self.username.clone());
            let table_pair = (schema, table_info.name.clone());
            let columns = table_columns.remove(&table_pair).ok_or_else(|| {
                Error::TableNotFound(TableIdentifier {
                    schema: table_info.schema.clone(),
                    name: table_info.name.clone(),
                })
            })?;
            result.push(mapping::decide_schema(
                &self.connection_name,
                table_info.schema.clone(),
                table_pair.1,
                &table_info.column_names,
                columns,
            ));
        }
        self.connection.commit()?;

        Ok(result)
    }

    pub fn snapshot(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<(usize, TableInfo)>,
    ) -> Result<Scn> {
        let schemas = self
            .get_schemas(tables.iter().map(|(_, table)| table))?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        debug!("{}", sql);
        self.connection.execute(sql, &[])?;

        for ((table_index, table), schema) in tables.into_iter().zip(schemas) {
            let columns = table
                .column_names
                .into_iter()
                .filter(|s| s != "INGESTED_AT")
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<String>>()
                .join(", ");
            let owner = table.schema.unwrap_or_else(|| self.username.clone());
            let sql = format!(
                "SELECT {}, NULL as \"INGESTED_AT\" FROM {}.{}",
                columns, owner, table.name
            );
            debug!("{}", sql);
            let rows = self.connection.query(&sql, &[])?;

            let mut batch = Vec::with_capacity(self.batch_size);
            for row in rows {
                batch.push(mapping::map_row(&schema.schema, row?)?);
                if batch.len() >= self.batch_size
                    && ingestor
                        .blocking_handle_message(IngestionMessage::OperationEvent {
                            table_index,
                            op: Operation::BatchInsert {
                                new: std::mem::take(&mut batch),
                            },
                            id: None,
                        })
                        .is_err()
                {
                    return self.get_scn_and_commit();
                }
            }

            if !batch.is_empty()
                && ingestor
                    .blocking_handle_message(IngestionMessage::OperationEvent {
                        table_index,
                        op: Operation::BatchInsert { new: batch },
                        id: None,
                    })
                    .is_err()
            {
                return self.get_scn_and_commit();
            }
        }

        self.get_scn_and_commit()
    }

    pub(crate) fn get_scn_and_commit(&mut self) -> Result<Scn> {
        let sql = "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER() FROM DUAL";
        let scn = self.connection.query_row_as::<Scn>(sql, &[])?;
        self.connection.commit()?;
        Ok(scn)
    }

    pub fn replicate(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<(usize, TableInfo)>,
        schemas: Vec<Schema>,
        checkpoint: Scn,
        con_id: Option<u32>,
    ) -> Result<()> {
        match self.replicator {
            OracleReplicator::LogMiner(config) => {
                self.replicate_log_miner(ingestor, tables, schemas, checkpoint, con_id, config)
            }
            OracleReplicator::DozerLogReader => unimplemented!("dozer log reader"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn replicate_log_miner(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<(usize, TableInfo)>,
        schemas: Vec<Schema>,
        checkpoint: Scn,
        con_id: Option<u32>,
        config: LogMinerConfig,
    ) -> Result<()> {
        let start_scn = checkpoint + 1;
        let table_pair_to_index = tables
            .into_iter()
            .map(|(index, table)| {
                let schema = table.schema.unwrap_or_else(|| self.username.clone());
                ((schema, table.name), index)
            })
            .collect::<HashMap<_, _>>();
        let processor = replicate::Processor::new(start_scn, table_pair_to_index, schemas);

        let (sender, receiver) = std::sync::mpsc::sync_channel(100);
        let handle = {
            let connection = self.connection.clone();
            let ingestor = ingestor.clone();
            std::thread::spawn(move || {
                replicate::log_miner_loop(&connection, start_scn, con_id, config, sender, &ingestor)
            })
        };

        for transaction in processor.process(receiver) {
            let transaction = match transaction {
                Ok(transaction) => transaction,
                Err(e) => {
                    error!("Error during transaction processing: {e}");
                    continue;
                }
            };

            for (seq, (table_index, op)) in transaction.operations.into_iter().enumerate() {
                if ingestor
                    .blocking_handle_message(IngestionMessage::OperationEvent {
                        table_index,
                        op,
                        id: Some(OpIdentifier::new(transaction.commit_scn, seq as u64)),
                    })
                    .is_err()
                {
                    return Ok(());
                };
            }

            if ingestor
                .blocking_handle_message(IngestionMessage::TransactionInfo(
                    TransactionInfo::Commit {
                        id: Some(OpIdentifier::new(transaction.commit_scn, 0)),
                        source_time: Some(SourceTime::from_chrono(
                            &transaction.commit_timestamp,
                            1000,
                        )),
                    },
                ))
                .is_err()
            {
                return Ok(());
            }
        }

        handle.join().unwrap()?;
        Ok(())
    }
}

mod join;
mod listing;
mod mapping;
mod replicate;

const TEMP_DOZER_TYPE_NAME: &str = "TEMP_DOZER_TYPE";

fn temp_varray_of_vchar2(
    connection: &Connection,
    num_strings: usize,
    max_num_chars: usize,
) -> Result<ObjectType> {
    let sql = format!(
        "CREATE OR REPLACE TYPE {} AS VARRAY({}) OF VARCHAR2({})",
        TEMP_DOZER_TYPE_NAME, num_strings, max_num_chars
    );
    debug!("{}", sql);
    connection.execute(&sql, &[])?;
    connection
        .object_type(TEMP_DOZER_TYPE_NAME)
        .map_err(Into::into)
}

fn string_collection(connection: &Connection, strings: &[String]) -> Result<Collection> {
    let temp_type = temp_varray_of_vchar2(
        connection,
        strings.len(),
        strings.iter().map(|s| s.len()).max().unwrap(),
    )?;
    let mut collection = temp_type.new_collection()?;
    for string in strings {
        collection.push(&OracleString(string))?;
    }
    Ok(collection)
}

#[cfg(test)]
mod tests {
    use dozer_ingestion_connector::futures::{Stream, StreamExt};

    #[tokio::test]
    #[ignore]
    async fn test_connector() {
        use dozer_ingestion_connector::dozer_types::{
            models::ingestion_types::IngestionMessage, types::Operation,
        };
        use dozer_ingestion_connector::{
            dozer_types::models::ingestion_types::OracleReplicator, IngestionConfig, Ingestor,
        };
        use std::time::Instant;

        fn row_count(message: &IngestionMessage) -> usize {
            match message {
                IngestionMessage::OperationEvent { op, .. } => match op {
                    Operation::BatchInsert { new } => new.len(),
                    Operation::Insert { .. } => 1,
                    Operation::Delete { .. } => 1,
                    Operation::Update { .. } => 1,
                },
                _ => 0,
            }
        }

        async fn estimate_throughput(mut iterator: impl Stream<Item = IngestionMessage> + Unpin) {
            let mut tic = None;
            let mut count = 0;
            let print_count_interval = 10_000;
            let mut count_mod_interval = 0;
            while let Some(message) = iterator.next().await {
                if tic.is_none() {
                    tic = Some(Instant::now());
                }

                count += row_count(&message);
                let new_count_mod_interval = count / print_count_interval;
                if new_count_mod_interval > count_mod_interval {
                    count_mod_interval = new_count_mod_interval;
                    println!("{} rows in {:?}", count, tic.unwrap().elapsed());
                }
            }
            println!("{} rows in {:?}", count, tic.unwrap().elapsed());
            println!(
                "Throughput: {} rows/s",
                count as f64 / tic.unwrap().elapsed().as_secs_f64()
            );
        }

        env_logger::init();

        let replicate_user = "C##DOZER";
        let data_user = "CHUBEI";
        let host = "localhost";
        let sid = "ORCLPDB1";

        let mut connector = super::Connector::new(
            "oracle".into(),
            replicate_user.into(),
            "123",
            &format!("{}:{}/{}", host, 1521, sid),
            100_000,
            OracleReplicator::DozerLogReader,
        )
        .unwrap();
        let tables = connector.list_tables(&[data_user.into()]).unwrap();
        let tables = connector.list_columns(tables).unwrap();
        let schemas = connector.get_schemas(&tables).unwrap();
        let schemas = schemas.into_iter().map(Result::unwrap).collect::<Vec<_>>();
        dbg!(&schemas);
        let tables: Vec<_> = tables.into_iter().enumerate().collect();
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let handle = {
            let tables = tables.clone();
            tokio::task::spawn_blocking(move || connector.snapshot(&ingestor, tables))
        };

        estimate_throughput(iterator).await;
        let checkpoint = handle.await.unwrap().unwrap();

        let sid = "ORCLCDB";
        let mut connector = super::Connector::new(
            "oracle".into(),
            replicate_user.into(),
            "123",
            &format!("{}:{}/{}", host, 1521, sid),
            1,
            OracleReplicator::LogMiner(Default::default()),
        )
        .unwrap();
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let schemas = schemas.into_iter().map(|schema| schema.schema).collect();
        let handle = tokio::spawn(async move {
            connector.replicate(&ingestor, tables, schemas, checkpoint, None)
        });

        estimate_throughput(iterator).await;
        handle.await.unwrap().unwrap();
    }
}
