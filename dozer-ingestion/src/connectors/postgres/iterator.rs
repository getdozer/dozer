use crate::connectors::connector::TableInfo;

pub mod iterator;
pub mod handler;

pub struct Details {
    id: u64,
    publication_name: String,
    slot_name: String,
    tables: Option<Vec<TableInfo>>,
    conn_str: String,
    conn_str_plain: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationState {
    Pending,
    SnapshotInProgress,
    Replicating,
}