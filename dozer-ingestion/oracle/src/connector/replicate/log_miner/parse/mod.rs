use std::{collections::HashMap, str::FromStr};

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::{trace, warn},
    rust_decimal::Decimal,
};

use crate::connector::{Error, Scn};

use super::listing::LogManagerContent;

#[derive(Debug, Clone)]
pub struct ParsedLogManagerContent {
    pub scn: Scn,
    pub timestamp: DateTime<Utc>,
    pub kind: ParsedLogManagerContentKind,
}

#[derive(Debug, Clone)]
pub enum ParsedLogManagerContentKind {
    Op {
        table_index: usize,
        op: ParsedOperation,
    },
    Commit,
}

type ParsedRow = HashMap<String, ParsedValue>;

#[derive(Debug, Clone)]
pub enum ParsedOperation {
    Insert(ParsedRow),
    Delete(ParsedRow),
    Update { old: ParsedRow, new: ParsedRow },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedValue {
    String(String),
    Number(Decimal),
    Null,
}

#[derive(Debug, Clone)]
pub struct Parser {
    insert_parser: insert::Parser,
    delete_parser: delete::Parser,
    update_parser: update::Parser,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            insert_parser: insert::Parser::new(),
            delete_parser: delete::Parser::new(),
            update_parser: update::Parser::new(),
        }
    }

    pub fn parse(
        &self,
        content: LogManagerContent,
        table_pair_to_index: &HashMap<(String, String), usize>,
    ) -> Result<Option<ParsedLogManagerContent>, Error> {
        if content.operation_code == OP_CODE_COMMIT {
            return Ok(Some(ParsedLogManagerContent {
                scn: content.commit_scn,
                timestamp: content.commit_timestamp,
                kind: ParsedLogManagerContentKind::Commit,
            }));
        }

        let Some(owner) = content.seg_owner else {
            return Ok(None);
        };
        let Some(table_name) = content.table_name else {
            return Ok(None);
        };
        let table_pair = (owner, table_name);
        let Some(&table_index) = table_pair_to_index.get(&table_pair) else {
            trace!(
                "Ignoring operation on table {}.{}",
                table_pair.0,
                table_pair.1
            );
            return Ok(None);
        };

        let op = match content.operation_code {
            OP_CODE_INSERT => ParsedOperation::Insert(self.insert_parser.parse(
                &content.sql_redo.expect("insert must have redo"),
                &table_pair,
            )?),
            OP_CODE_DELETE => ParsedOperation::Delete(self.delete_parser.parse(
                &content.sql_redo.expect("delete must have redo"),
                &table_pair,
            )?),
            OP_CODE_UPDATE => {
                let (old, new) = self.update_parser.parse(
                    &content.sql_redo.expect("update must have redo"),
                    &table_pair,
                )?;
                ParsedOperation::Update { old, new }
            }
            OP_CODE_DDL => {
                warn!("Ignoring DDL operation: {:?}", content.sql_redo);
                return Ok(None);
            }
            _ => {
                trace!("Ignoring operation: {:?}", content.sql_redo);
                return Ok(None);
            }
        };

        Ok(Some(ParsedLogManagerContent {
            scn: content.commit_scn,
            timestamp: content.commit_timestamp,
            kind: ParsedLogManagerContentKind::Op { table_index, op },
        }))
    }
}

impl FromStr for ParsedValue {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with('\'') {
            Ok(ParsedValue::String(s[1..s.len() - 1].to_string()))
        } else {
            Ok(ParsedValue::Number(s.parse()?))
        }
    }
}

mod delete;
mod insert;
mod row;
mod update;

const OP_CODE_INSERT: u8 = 1;
const OP_CODE_DELETE: u8 = 2;
const OP_CODE_UPDATE: u8 = 3;
const OP_CODE_DDL: u8 = 5;
const OP_CODE_COMMIT: u8 = 7;
