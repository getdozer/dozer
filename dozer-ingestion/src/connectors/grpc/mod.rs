#[allow(dead_code)]
pub mod connector;
mod ingest;

mod adapter;
pub use adapter::{ArrowAdapter, DefaultAdapter, GrpcIngestMessage, GrpcIngestor, IngestAdapter};

#[cfg(test)]
mod tests;
