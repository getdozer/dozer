use std::fmt::Debug;

mod default;

mod arrow;

pub use arrow::ArrowAdapter;
pub use default::DefaultAdapter;
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::grpc_types::ingest::{IngestArrowRequest, IngestRequest},
    Ingestor, SourceSchema,
};

use crate::Error;

#[async_trait]
pub trait IngestAdapter: Debug
where
    Self: Send + Sync + 'static + Sized,
{
    fn new(schemas_str: String) -> Result<Self, Error>;
    fn get_schemas(&self) -> Vec<(String, SourceSchema)>;
    async fn handle_message(
        &self,
        table_index: usize,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), Error>;
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
    pub fn new(schemas_str: String) -> Result<Self, Error> {
        let adapter = T::new(schemas_str)?;
        Ok(Self { adapter })
    }
}

impl<A> GrpcIngestor<A>
where
    A: IngestAdapter,
{
    pub fn get_schemas(&self) -> Result<Vec<(String, SourceSchema)>, Error> {
        Ok(self.adapter.get_schemas())
    }

    pub async fn handle_message(
        &self,
        table_index: usize,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), Error> {
        self.adapter
            .handle_message(table_index, msg, ingestor)
            .await
    }
}
