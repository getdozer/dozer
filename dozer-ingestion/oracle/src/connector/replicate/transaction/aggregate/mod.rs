use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::{trace, warn},
};
use fxhash::FxHashSet;

use crate::connector::{
    replicate::log::{LogMinerContent, OperationType, TransactionId},
    Scn,
};

#[derive(Debug, Clone)]
pub struct Transaction {
    pub commit_scn: Scn,
    pub commit_timestamp: DateTime<Utc>,
    pub operations: Vec<RawOperation>,
}

#[derive(Debug, Clone)]
pub struct RawOperation {
    pub seg_owner: String,
    pub table_name: String,
    pub kind: OperationKind,
    pub sql_redo: String,
}

#[derive(Debug, Clone, Copy)]
pub enum OperationKind {
    Insert,
    Delete,
    Update,
}

#[derive(Debug, Clone)]
pub struct Aggregator {
    start_scn: Scn,
    table_pairs: FxHashSet<(String, String)>,
}

impl Aggregator {
    pub fn new<'a>(
        start_scn: Scn,
        table_pairs_to_index: impl Iterator<Item = &'a (String, String)>,
    ) -> Self {
        Self {
            start_scn,
            table_pairs: table_pairs_to_index.cloned().collect(),
        }
    }

    pub fn process(
        &self,
        iterator: impl Iterator<Item = LogMinerContent>,
    ) -> impl Iterator<Item = Transaction> {
        Processor {
            table_pairs: self.table_pairs.clone(),
            iterator,
            start_scn: self.start_scn,
            transaction_forest: Default::default(),
        }
    }
}

type TransactionForest = forest::Forest<TransactionId, Vec<RawOperation>>;

#[derive(Debug)]
struct Processor<I: Iterator<Item = LogMinerContent>> {
    iterator: I,
    start_scn: Scn,
    table_pairs: FxHashSet<(String, String)>,
    transaction_forest: TransactionForest,
}

impl<I: Iterator<Item = LogMinerContent>> Iterator for Processor<I> {
    type Item = Transaction;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let content = self.iterator.next()?;

            if content.operation_type == OperationType::Commit {
                if let Some(transaction) = commit::commit(
                    content.xid,
                    content.pxid,
                    content.scn,
                    content.timestamp,
                    &mut self.transaction_forest,
                ) {
                    if transaction.commit_scn >= self.start_scn {
                        return Some(transaction);
                    }
                }
                continue;
            }

            if content.operation_type == OperationType::Rollback {
                self.transaction_forest
                    .remove_subtree(content.xid, |_, _| ());
                continue;
            }

            let Some(seg_owner) = content.seg_owner else {
                continue;
            };
            let Some(table_name) = content.table_name else {
                continue;
            };
            if !self
                .table_pairs
                .contains(&(seg_owner.clone(), table_name.clone()))
            {
                continue;
            }
            let (kind, sql_redo) = match content.operation_type {
                OperationType::Insert => (
                    OperationKind::Insert,
                    content.sql_redo.expect("insert must have redo"),
                ),
                OperationType::Delete => (
                    OperationKind::Delete,
                    content.sql_redo.expect("delete must have redo"),
                ),
                OperationType::Update => (
                    OperationKind::Update,
                    content.sql_redo.expect("update must have redo"),
                ),
                OperationType::Ddl => {
                    warn!("Ignoring DDL operation: {:?}", content.sql_redo);
                    continue;
                }
                _ => {
                    trace!("Ignoring operation: {:?}", content.sql_redo);
                    continue;
                }
            };
            op::process_operation(
                content.xid,
                content.pxid,
                RawOperation {
                    seg_owner,
                    table_name,
                    kind,
                    sql_redo,
                },
                &mut self.transaction_forest,
            );
        }
    }
}

mod commit;
mod forest;
mod op;
