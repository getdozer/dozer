use std::marker::PhantomData;

use dozer_types::{
    grpc_types::ingest::{IngestArrowRequest, IngestRequest},
    types::SourceSchema,
};

use crate::errors::ConnectorError;

use self::{arrow::ArrowAdapter, default::DefaultAdapter};

mod default;

mod arrow;

pub trait GrpcIngestAdapter<T>: Send + Sync {
    fn get_schemas(&self, schemas_str: &String) -> Result<Vec<SourceSchema>, ConnectorError>;
    fn handle_message(&self, msg: T) -> Result<(), ConnectorError>;
}

pub fn get_adapter<T>(format: &str) -> Result<Box<dyn GrpcIngestAdapter<T>>, ConnectorError> {
    match format {
        "arrow" => Ok(Box::new(ArrowAdapter::<IngestArrowRequest> {
            arrow_schemas: vec![],
            phantom: PhantomData,
        })),
        "default" => Ok(Box::new(DefaultAdapter::<IngestRequest> {
            phantom: PhantomData,
        })),
        _ => Err(ConnectorError::InitializationError(format!(
            "Grpc Adapter {} not found",
            format
        ))),
    }
}

// pub fn insert(
//     req: IngestRequest,
//     schema_map: &'static HashMap<String, Schema>,
//     ingestor: &'static Ingestor,
// ) -> Result<(), tonic::Status> {
//     let schema = schema_map.get(&req.schema_name).ok_or_else(|| {
//         tonic::Status::invalid_argument(format!("schema not found: {}", req.schema_name))
//     })?;

//     let seq_no = req.seq_no;
//     let records = map_record_batch(req, schema)?;

//     for r in records {
//         let op = Operation::Insert { new: r };
//         ingestor
//             .handle_message(IngestionMessage::new_op(0, seq_no as u64, op))
//             .map_err(|e| tonic::Status::internal(format!("ingestion error: {e}")))?;
//     }

//     Ok(())
// }
