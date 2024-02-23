use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::warn,
};

use crate::connector::{replicate::log::TransactionId, Scn};

use super::{Transaction, TransactionForest};

pub fn commit(
    xid: TransactionId,
    pxid: TransactionId,
    scn: Scn,
    timestamp: DateTime<Utc>,
    transaction_forest: &mut TransactionForest,
) -> Option<Transaction> {
    let mut operations = vec![];
    transaction_forest.remove_subtree(xid, |_, ops| operations.extend(ops));

    if xid == pxid {
        // This is a top level transaciton
        Some(Transaction {
            commit_scn: scn,
            commit_timestamp: timestamp,
            operations,
        })
    } else {
        // This is a sub transaction.
        let Some(parent_operations) = transaction_forest.get_mut(&pxid) else {
            warn!(
                "Parent transaction {:02X?} not found for sub transaction {:02X?}",
                pxid, xid
            );
            return None;
        };
        parent_operations.extend(operations);
        None
    }
}
