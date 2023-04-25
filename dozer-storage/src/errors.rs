#![allow(clippy::enum_variant_names)]
use std::thread::ThreadId;

use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Bad map size: {map_size}, must be a multiple of system page size, which is currently {page_size}")]
    BadPageSize { map_size: usize, page_size: usize },
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Transaction was created in thread: {create_thread_id:?}, but committed in thread: {commit_thread_id:?}")]
    TransactionCommittedAcrossThread {
        create_thread_id: ThreadId,
        commit_thread_id: ThreadId,
    },

    #[error("Unable to deserialize type: {} - Reason: {}", typ, reason.to_string())]
    DeserializationError {
        typ: &'static str,
        reason: BoxedError,
    },
    #[error("Unable to serialize type: {} - Reason: {}", typ, reason.to_string())]
    SerializationError {
        typ: &'static str,
        reason: BoxedError,
    },

    // Error forwarding
    #[error("Lmdb error: {0}")]
    Lmdb(#[from] lmdb::Error),
}
