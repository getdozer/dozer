use std::{collections::HashMap, str::FromStr};

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::trace,
    rust_decimal::Decimal,
};

use crate::connector::{Error, Scn};

use super::aggregate::{Operation, OperationKind, Transaction};

#[derive(Debug, Clone)]
pub struct ParsedTransaction {
    pub commit_scn: Scn,
    pub commit_timestamp: DateTime<Utc>,
    pub operations: Vec<ParsedOperation>,
}

#[derive(Debug, Clone)]
pub struct ParsedOperation {
    pub table_index: usize,
    pub kind: ParsedOperationKind,
}

pub type ParsedRow = HashMap<String, ParsedValue>;

#[derive(Debug, Clone)]
pub enum ParsedOperationKind {
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
    table_pair_to_index: HashMap<(String, String), usize>,
}

impl Parser {
    pub fn new(table_pair_to_index: HashMap<(String, String), usize>) -> Self {
        Self {
            insert_parser: insert::Parser::new(),
            delete_parser: delete::Parser::new(),
            update_parser: update::Parser::new(),
            table_pair_to_index,
        }
    }

    pub fn process<'a>(
        &'a self,
        iterator: impl Iterator<Item = Transaction> + 'a,
    ) -> impl Iterator<Item = Result<ParsedTransaction, Error>> + 'a {
        Processor {
            iterator,
            parser: self,
        }
    }

    fn parse(&self, operation: Operation) -> Result<Option<ParsedOperation>, Error> {
        let table_pair = (operation.seg_owner, operation.table_name);
        let Some(&table_index) = self.table_pair_to_index.get(&table_pair) else {
            trace!(
                "Ignoring operation on table {}.{}",
                table_pair.0,
                table_pair.1
            );
            return Ok(None);
        };

        let kind = match operation.kind {
            OperationKind::Insert => ParsedOperationKind::Insert(
                self.insert_parser.parse(&operation.sql_redo, &table_pair)?,
            ),
            OperationKind::Delete => ParsedOperationKind::Delete(
                self.delete_parser.parse(&operation.sql_redo, &table_pair)?,
            ),
            OperationKind::Update => {
                let (old, new) = self.update_parser.parse(&operation.sql_redo, &table_pair)?;
                ParsedOperationKind::Update { old, new }
            }
        };
        Ok(Some(ParsedOperation { table_index, kind }))
    }
}

#[derive(Debug)]
struct Processor<'a, I: Iterator<Item = Transaction>> {
    iterator: I,
    parser: &'a Parser,
}

impl<'a, I: Iterator<Item = Transaction>> Iterator for Processor<'a, I> {
    type Item = Result<ParsedTransaction, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let transaction = self.iterator.next()?;

        let mut operations = vec![];
        for operation in transaction.operations {
            match self.parser.parse(operation) {
                Ok(Some(operation)) => operations.push(operation),
                Ok(None) => continue,
                Err(err) => return Some(Err(err)),
            }
        }

        Some(Ok(ParsedTransaction {
            commit_scn: transaction.commit_scn,
            commit_timestamp: transaction.commit_timestamp,
            operations,
        }))
    }
}

impl FromStr for ParsedValue {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with('\'') {
            Ok(ParsedValue::String(s[1..s.len() - 1].to_string()))
        } else {
            Ok(ParsedValue::Number(s.parse().map_err(|e| {
                crate::connector::Error::NumberToDecimal(e, s.to_string())
            })?))
        }
    }
}

mod delete;
mod insert;
mod row;
mod update;
