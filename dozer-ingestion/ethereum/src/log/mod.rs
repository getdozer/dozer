mod connector;
mod helper;
mod sender;
pub use connector::EthLogConnector;
use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};

#[cfg(test)]
mod tests;

#[derive(Debug, Error)]
enum Error {
    #[error("Failed fetching after {0} recursions")]
    EthTooManyRecurisions(usize),

    #[error("Received empty message in connector")]
    EmptyMessage,
}
