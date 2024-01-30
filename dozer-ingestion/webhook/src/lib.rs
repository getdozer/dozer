use std::{net::AddrParseError, path::PathBuf};

use dozer_ingestion_connector::dozer_types::{
    serde_json,
    thiserror::{self, Error},
};
pub mod connector;
mod server;
#[cfg(test)]
mod tests;
mod util;
#[derive(Debug, Error)]

pub enum Error {
    #[error("cannot read file {0:?}: {1}")]
    CannotReadFile(PathBuf, #[source] std::io::Error),
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("arrow error: {0}")]
    AddrParse(#[from] AddrParseError),
    #[error("default adapter cannot handle arrow ingest message")]
    SchemaNotFound(String),
    #[error("field {0} not found in schema")]
    FieldNotFound(String),
    #[error("actix web start error: {0}")]
    ActixWebStartError(#[from] std::io::Error),
}
