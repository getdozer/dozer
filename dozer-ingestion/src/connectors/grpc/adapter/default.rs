use std::marker::PhantomData;

use dozer_types::{
    grpc_types::ingest::IngestRequest,
    serde_json,
    types::{SchemaIdentifier, SourceSchema},
};

use crate::errors::ConnectorError;

use super::GrpcIngestAdapter;

#[derive(Debug)]
pub struct DefaultAdapter<T> {
    phantom: PhantomData<T>,
}

impl<T> DefaultAdapter<T> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl GrpcIngestAdapter<IngestRequest> for DefaultAdapter<IngestRequest> {
    fn get_schemas(&self, schemas_str: &String) -> Result<Vec<SourceSchema>, ConnectorError> {
        let mut schemas: Vec<SourceSchema> =
            serde_json::from_str(&schemas_str).map_err(ConnectorError::map_serialization_error)?;

        schemas = schemas
            .iter()
            .enumerate()
            .map(|(id, schema)| {
                let mut s = schema.clone();
                s.schema.identifier = Some(SchemaIdentifier {
                    id: id as u32,
                    version: 1,
                });
                s
            })
            .collect();
        Ok(schemas)
    }
    fn handle_message(&self, msg: IngestRequest) -> Result<(), ConnectorError> {
        todo!()
    }
}
