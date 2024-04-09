use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::{trace, warn},
};
use fxhash::FxHashSet;

use crate::connector::{
    replicate::log::{LogManagerContent, TransactionId},
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
        iterator: impl Iterator<Item = LogManagerContent>,
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
struct Processor<I: Iterator<Item = LogManagerContent>> {
    iterator: I,
    start_scn: Scn,
    table_pairs: FxHashSet<(String, String)>,
    transaction_forest: TransactionForest,
}

impl<I: Iterator<Item = LogManagerContent>> Iterator for Processor<I> {
    type Item = Transaction;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let content = self.iterator.next()?;

            if content.operation_code == OP_CODE_COMMIT {
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

            if content.operation_code == OP_CODE_ROLLBACK {
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
            let (kind, sql_redo) = match content.operation_code {
                OP_CODE_INSERT => (
                    OperationKind::Insert,
                    content.sql_redo.expect("insert must have redo"),
                ),
                OP_CODE_DELETE => (
                    OperationKind::Delete,
                    content.sql_redo.expect("delete must have redo"),
                ),
                OP_CODE_UPDATE => (
                    OperationKind::Update,
                    content.sql_redo.expect("update must have redo"),
                ),
                OP_CODE_DDL => {
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

const OP_CODE_INSERT: u8 = 1;
const OP_CODE_DELETE: u8 = 2;
const OP_CODE_UPDATE: u8 = 3;
const OP_CODE_DDL: u8 = 5;
const OP_CODE_COMMIT: u8 = 7;
const OP_CODE_ROLLBACK: u8 = 36;
