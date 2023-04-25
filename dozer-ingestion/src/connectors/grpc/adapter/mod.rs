use dozer_types::grpc_types::ingest::{IngestArrowRequest, IngestRequest};

use crate::{connectors::SourceSchema, errors::ConnectorError, ingestion::Ingestor};

mod default;

mod arrow;

pub use arrow::ArrowAdapter;
pub use default::DefaultAdapter;
pub trait IngestAdapter
where
    Self: Send + Sync + 'static + Sized,
{
    fn new(schemas_str: String) -> Result<Self, ConnectorError>;
    fn get_schemas(&self) -> Vec<(String, SourceSchema)>;
    fn handle_message(
        &self,
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

    pub fn handle_message(
        &self,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), ConnectorError> {
        self.adapter.handle_message(msg, ingestor)
    }
}
