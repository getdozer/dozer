use std::{borrow::Cow, collections::HashMap};

use dozer_ingestion_connector::dozer_types::{
    log::trace,
    types::{Operation, Schema},
};
use fxhash::FxHashMap;

use crate::connector::{
    replicate::transaction::{map::map_row, parse::insert::DmlParser},
    Error,
};

use super::{
    aggregate::{OperationKind, RawOperation, Transaction},
    ParsedTransaction,
};
pub type ParsedRow<'a> = Vec<Option<Cow<'a, str>>>;

#[derive(Debug, Clone)]
struct TableInfo {
    index: usize,
    column_indices: FxHashMap<String, usize>,
    schema: Schema,
}
impl TableInfo {
    fn new(index: usize, schema: Schema) -> Self {
        let column_indices = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| (field.name.clone(), i))
            .collect();
        Self {
            index,
            schema,
            column_indices,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Parser {
    table_infos: FxHashMap<(String, String), TableInfo>,
}

impl Parser {
    pub fn new(
        table_pair_to_index: HashMap<(String, String), usize>,
        schemas: Vec<Schema>,
    ) -> Self {
        let table_infos = table_pair_to_index
            .into_iter()
            .map(|(k, v)| (k, TableInfo::new(v, schemas[v].clone())))
            .collect();
        Self { table_infos }
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

    fn parse(&self, operation: RawOperation) -> Result<Option<(usize, Operation)>, Error> {
        let table_pair = (operation.seg_owner, operation.table_name);
        let Some(&table_info) = self.table_infos.get(&table_pair).as_ref() else {
            trace!(
                "Ignoring operation on table {}.{}",
                table_pair.0,
                table_pair.1
            );
            return Ok(None);
        };

        trace!(target: "oracle_replication_parser", "Parsing operation on table {}.{}", table_pair.0, table_pair.1);

        let mut parser = DmlParser::new(&operation.sql_redo, &table_info.column_indices);
        let op = match operation.kind {
            OperationKind::Insert => {
                let new_values = parser
                    .parse_insert()
                    .ok_or_else(|| Error::InsertFailedToMatch(operation.sql_redo.clone()))?;
                Operation::Insert {
                    new: map_row(new_values, &table_info.schema)?,
                }
            }
            OperationKind::Delete => {
                let old = parser
                    .parse_delete()
                    .ok_or_else(|| Error::DeleteFailedToMatch(operation.sql_redo.clone()))?;
                Operation::Delete {
                    old: map_row(old, &table_info.schema)?,
                }
            }
            OperationKind::Update => {
                let (old, new) = parser
                    .parse_update()
                    .ok_or_else(|| Error::UpdateFailedToMatch(operation.sql_redo.clone()))?;
                Operation::Update {
                    old: map_row(old, &table_info.schema)?,
                    new: map_row(new, &table_info.schema)?,
                }
            }
        };
        Ok(Some((table_info.index, op)))
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

mod delete;
mod insert;
mod row;
mod update;
