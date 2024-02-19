use std::{collections::HashMap, sync::mpsc::SyncSender};

use dozer_ingestion_connector::dozer_types::{self, types::Schema};
use oracle::Connection;

use crate::connector::{Error, Scn};

use self::parse::{ParsedLogManagerContentKind, ParsedOperation};

use super::listing::ArchivedLog;

#[derive(Debug, Clone)]
pub enum MappedLogManagerContent {
    Commit(Scn),
    Op {
        scn: Scn,
        table_index: usize,
        op: dozer_types::types::Operation,
    },
}

#[derive(Debug, Clone)]
pub struct LogMiner {
    parser: parse::Parser,
}

impl LogMiner {
    pub fn new() -> Self {
        Self {
            parser: parse::Parser::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mine(
        &self,
        connection: &Connection,
        log: &ArchivedLog,
        start_scn: Scn,
        table_pair_to_index: &HashMap<(String, String), usize>,
        schemas: &[Schema],
        con_id: Option<u32>,
        output: SyncSender<Result<MappedLogManagerContent, Error>>,
    ) {
        let _guard = match start_log_manager(connection, log) {
            Ok(guard) => guard,
            Err(e) => {
                let _ = output.send(Err(e));
                return;
            }
        };

        let contents = match listing::LogManagerContent::list(connection, start_scn, con_id) {
            Ok(contents) => contents,
            Err(e) => {
                let _ = output.send(Err(e));
                return;
            }
        };
        for content in contents {
            match parse_and_map(&self.parser, content, table_pair_to_index, schemas) {
                Ok(Some(mapped)) => {
                    if output.send(Ok(mapped)).is_err() {
                        return;
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    let _ = output.send(Err(err));
                    return;
                }
            }
        }

        if let Err(e) = end_log_manager(connection) {
            let _ = output.send(Err(e));
        }
    }
}

mod listing;
mod mapping;
mod parse;

fn start_log_manager<'a>(
    connection: &'a Connection,
    log: &ArchivedLog,
) -> Result<LogManagerGuard<'a>, Error> {
    let sql =
        "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :name, OPTIONS => DBMS_LOGMNR.NEW); END;";
    connection.execute(sql, &[&str_to_sql!(log.name)])?;
    let sql = "
    BEGIN
        DBMS_LOGMNR.START_LOGMNR(
            OPTIONS =>
                DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                DBMS_LOGMNR.PRINT_PRETTY_SQL +
                DBMS_LOGMNR.NO_ROWID_IN_STMT
        );
    END;";
    connection.execute(sql, &[])?;
    Ok(LogManagerGuard { connection })
}

fn end_log_manager(connection: &Connection) -> Result<(), Error> {
    connection.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;", &[])?;
    Ok(())
}

#[derive(Debug)]
struct LogManagerGuard<'a> {
    connection: &'a Connection,
}

impl<'a> Drop for LogManagerGuard<'a> {
    fn drop(&mut self) {
        let _ = end_log_manager(self.connection);
    }
}

fn parse_and_map(
    parser: &parse::Parser,
    content: Result<listing::LogManagerContent, Error>,
    table_pair_to_index: &HashMap<(String, String), usize>,
    schemas: &[Schema],
) -> Result<Option<MappedLogManagerContent>, Error> {
    let Some(parsed) = parser.parse(content?, table_pair_to_index)? else {
        return Ok(None);
    };
    Ok(Some(match parsed.kind {
        ParsedLogManagerContentKind::Commit => MappedLogManagerContent::Commit(parsed.scn),
        ParsedLogManagerContentKind::Op { table_index, op } => {
            let schema = &schemas[table_index];
            use dozer_types::types::Operation;
            use mapping::map_row;

            let op = match op {
                ParsedOperation::Insert(row) => Operation::Insert {
                    new: map_row(row, schema)?,
                },
                ParsedOperation::Delete(row) => Operation::Delete {
                    old: map_row(row, schema)?,
                },
                ParsedOperation::Update { old, new } => Operation::Update {
                    old: map_row(old, schema)?,
                    new: map_row(new, schema)?,
                },
            };
            MappedLogManagerContent::Op {
                table_index,
                op,
                scn: parsed.scn,
            }
        }
    }))
}
