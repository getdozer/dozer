use dozer_ingestion_connector::dozer_types::log::warn;

use crate::connector::replicate::log::TransactionId;

use super::{RawOperation, TransactionForest};

pub fn process_operation(
    xid: TransactionId,
    pxid: TransactionId,
    operation: RawOperation,
    transaction_forest: &mut TransactionForest,
) {
    if xid == pxid {
        // This is a top level transaction
        transaction_forest.insert_or_get_root(xid).push(operation);
    } else {
        // This is a sub transaction.
        let Some(operations) = transaction_forest.insert_or_get_child(pxid, xid) else {
            warn!(
                "Parent transaction {:02X?} not found for sub transaction {:02X?}",
                pxid, xid
            );
            return;
        };
        operations.push(operation);
    }
}
