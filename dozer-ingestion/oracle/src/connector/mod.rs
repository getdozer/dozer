use std::{
    collections::{HashMap, HashSet},
    num::ParseFloatError,
    sync::Arc,
    time::Duration,
};

use dozer_ingestion_connector::{
    dozer_types::{
        chrono,
        epoch::SourceTime,
        log::{debug, error},
        models::ingestion_types::{IngestionMessage, OracleReplicator, TransactionInfo},
        node::OpIdentifier,
        rust_decimal, thiserror,
        types::{Operation, Schema},
    },
    Ingestor, SourceSchema, TableIdentifier, TableInfo,
};
use oracle::{
    sql_type::{Collection, ObjectType},
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseDateError {
    #[error("Invalid date format: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("Invalid oracle format")]
    Oracle,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("oracle error: {0:?}")]
    Oracle(#[from] oracle::Error),
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
}

/// `oracle`'s `ToSql` implementation for `&str` uses `NVARCHAR2` type, which Oracle expects to be UTF16 encoded by default.
/// Here we use `VARCHAR2` type instead, which Oracle expects to be UTF8 encoded by default.
/// This is a macro because it references a temporary `OracleType`.
macro_rules! str_to_sql {
    ($s:expr) => {
        // `s.len()` is the upper bound of `s.chars().count()`
        (
            &$s,
            &::oracle::sql_type::OracleType::Varchar2($s.len() as u32),
        )
    };
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
    ) -> Result<Self, Error> {
        let connection = Connection::connect(&username, password, connect_string)?;

        Ok(Self {
            connection_name,
            connection: Arc::new(connection),
            username,
            batch_size,
            replicator,
        })
    }

    pub fn get_con_id(&mut self, pdb: &str) -> Result<u32, Error> {
        let sql = "SELECT CON_NAME_TO_ID(:1) FROM DUAL";
        let con_id = self
            .connection
            .query_row_as::<Option<u32>>(sql, &[&str_to_sql!(pdb)])?
            .ok_or_else(|| Error::PdbNotFound(pdb.to_string()));
        self.connection.commit()?;
        con_id
    }

    pub fn list_tables(&mut self, schemas: &[String]) -> Result<Vec<TableIdentifier>, Error> {
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

    pub fn list_columns(&mut self, tables: Vec<TableIdentifier>) -> Result<Vec<TableInfo>, Error> {
        // List all tables and columns.
        let schemas = tables
            .iter()
            .map(|table| {
                table
                    .schema
                    .clone()
                    .unwrap_or_else(|| self.username.clone())
            })
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
    ) -> Result<Vec<Result<SourceSchema, Error>>, Error> {
        let table_infos: Vec<_> = table_infos.into_iter().collect();
        // Collect all tables and columns.
        let schemas = table_infos
            .iter()
            .map(|table| {
                table
                    .schema
                    .clone()
                    .unwrap_or_else(|| self.username.clone())
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let table_columns = listing::TableColumn::list(&self.connection, &schemas)?;
        let constraint_columns =
            listing::ConstraintColumn::list(&self.connection, &schemas).unwrap();
        let constraints = listing::Constraint::list(&self.connection, &schemas).unwrap();
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
    ) -> Result<Scn, Error> {
        let schemas = self
            .get_schemas(tables.iter().map(|(_, table)| table))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        debug!("{}", sql);
        self.connection.execute(sql, &[])?;

        for ((table_index, table), schema) in tables.into_iter().zip(schemas) {
            let columns = table.column_names.join(", ");
            let owner = table.schema.unwrap_or_else(|| self.username.clone());
            let sql = format!("SELECT {} FROM {}.{}", columns, owner, table.name);
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

    pub(crate) fn get_scn_and_commit(&mut self) -> Result<Scn, Error> {
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
    ) {
        match self.replicator {
            OracleReplicator::LogMiner {
                poll_interval_in_milliseconds,
                fetch_batch_size,
            } => self.replicate_log_miner(
                ingestor,
                tables,
                schemas,
                checkpoint,
                con_id,
                Duration::from_millis(poll_interval_in_milliseconds),
                fetch_batch_size.unwrap_or(1000),
            ),
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
        poll_interval: Duration,
        fetch_batch_size: u32,
    ) {
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
                replicate::log_miner_loop(
                    &connection,
                    start_scn,
                    con_id,
                    poll_interval,
                    fetch_batch_size,
                    sender,
                    &ingestor,
                )
            })
        };

        let mut recv = receiver.into_iter();
        let first = processor
            .process(recv.by_ref())
            .find(|op| !op.as_ref().unwrap().operations.is_empty())
            .unwrap()
            .unwrap();
        for transaction in recv.map(|_| Ok::<_, Error>(first.clone())) {
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
                    return;
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
                return;
            }
        }

        handle.join().unwrap();
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
) -> Result<ObjectType, Error> {
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

fn string_collection(connection: &Connection, strings: &[String]) -> Result<Collection, Error> {
    let temp_type = temp_varray_of_vchar2(
        connection,
        strings.len(),
        strings.iter().map(|s| s.len()).max().unwrap(),
    )?;
    let mut collection = temp_type.new_collection()?;
    for string in strings {
        collection.push(&str_to_sql!(*string))?;
    }
    Ok(collection)
}

mod tests {
    #[test]
    #[ignore]
    fn test_connector() {
        use dozer_ingestion_connector::{
            dozer_types::models::ingestion_types::OracleReplicator, IngestionConfig, Ingestor,
        };
        use dozer_ingestion_connector::{
            dozer_types::{models::ingestion_types::IngestionMessage, types::Operation},
            IngestionIterator,
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

        fn estimate_throughput(iterator: IngestionIterator) {
            let mut tic = None;
            let mut count = 0;
            let print_count_interval = 10_000;
            let mut count_mod_interval = 0;
            for message in iterator {
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
            std::thread::spawn(move || connector.snapshot(&ingestor, tables))
        };

        estimate_throughput(iterator);
        let checkpoint = handle.join().unwrap().unwrap();

        let sid = "ORCLCDB";
        let mut connector = super::Connector::new(
            "oracle".into(),
            replicate_user.into(),
            "123",
            &format!("{}:{}/{}", host, 1521, sid),
            1,
            OracleReplicator::LogMiner {
                poll_interval_in_milliseconds: 1000,
                fetch_batch_size: None,
            },
        )
        .unwrap();
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let schemas = schemas.into_iter().map(|schema| schema.schema).collect();
        let handle = std::thread::spawn(move || {
            connector.replicate(&ingestor, tables, schemas, checkpoint, None)
        });

        estimate_throughput(iterator);
        handle.join().unwrap();
    }
}
