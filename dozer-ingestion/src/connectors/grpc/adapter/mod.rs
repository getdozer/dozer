use std::fmt::Debug;

use dozer_types::grpc_types::ingest::{IngestArrowRequest, IngestRequest};
use dozer_types::tonic::async_trait;

use crate::{connectors::SourceSchema, errors::ConnectorError, ingestion::Ingestor};

mod default;

mod arrow;

pub use arrow::ArrowAdapter;
pub use default::DefaultAdapter;

#[async_trait]
pub trait IngestAdapter: Debug
where
    Self: Send + Sync + 'static + Sized,
{
    fn new(schemas_str: String) -> Result<Self, ConnectorError>;
    fn get_schemas(&self) -> Vec<(String, SourceSchema)>;
    async fn handle_message(
        &self,
        table_index: usize,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), ConnectorError>;
}

pub enum GrpcIngestMessage {
    Default(IngestRequest),
    Arrow(IngestArrowRequest),
}
pub struct GrpcIngestor<A>
where
    A: IngestAdapter,
{
    adapter: A,
}
impl<T> GrpcIngestor<T>
where
    T: IngestAdapter,
{
    pub fn new(schemas_str: String) -> Result<Self, ConnectorError> {
        let adapter = T::new(schemas_str)?;
        Ok(Self { adapter })
    }
}

impl<A> GrpcIngestor<A>
where
    A: IngestAdapter,
{
    pub fn get_schemas(&self) -> Result<Vec<(String, SourceSchema)>, ConnectorError> {
        Ok(self.adapter.get_schemas())
    }

    pub async fn handle_message(
        &self,
        table_index: usize,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), ConnectorError> {
        self.adapter
            .handle_message(table_index, msg, ingestor)
            .await
    }
}
