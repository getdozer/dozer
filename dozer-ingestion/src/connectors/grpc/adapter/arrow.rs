use std::collections::HashMap;

use dozer_types::{
    arrow::ipc::reader::StreamReader,
    arrow_types::{self, from_arrow::map_record_batch_to_dozer_records},
    bytes::{Buf, Bytes},
    grpc_types::ingest::IngestArrowRequest,
    ingestion_types::{GrpcArrowSchema, IngestionMessage},
    serde_json,
    types::{Operation, Record, Schema, SchemaIdentifier, SourceSchema},
};

use crate::{errors::ConnectorError, ingestion::Ingestor};

use super::{GrpcIngestMessage, IngestAdapter};

// Input is a JSON string or a path to a JSON file
// Takes name, arrow schema, and optionally replication type

#[derive(Debug)]
pub struct ArrowAdapter {}

impl ArrowAdapter {}

impl IngestAdapter for ArrowAdapter {
    fn new() -> Self {
        Self {}
    }

    fn get_schemas(&self, schemas_str: &str) -> Result<Vec<SourceSchema>, ConnectorError> {
        let grpc_schemas: Vec<GrpcArrowSchema> =
            serde_json::from_str(schemas_str).map_err(ConnectorError::map_serialization_error)?;
        let mut schemas = vec![];

        for (id, grpc_schema) in grpc_schemas.into_iter().enumerate() {
            let mut schema = arrow_types::from_arrow::map_schema_to_dozer(&grpc_schema.schema)
                .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
            schema.identifier = Some(SchemaIdentifier {
                id: id as u32,
                version: 1,
            });

            schemas.push(SourceSchema {
                name: grpc_schema.name,
                schema,
                replication_type: grpc_schema.replication_type.clone(),
            });
        }
        Ok(schemas)
    }
    fn handle_message(
        &self,
        msg: GrpcIngestMessage,
        schema_map: &'static HashMap<String, Schema>,
        ingestor: &'static Ingestor,
    ) -> Result<(), ConnectorError> {
        match msg {
            GrpcIngestMessage::Default(_) => Err(ConnectorError::InitializationError(
                "Wrong message format!".to_string(),
            )),
            GrpcIngestMessage::Arrow(msg) => handle_message(msg, schema_map, ingestor),
        }
    }
}

pub fn handle_message(
    req: IngestArrowRequest,
    schema_map: &'static HashMap<String, Schema>,
    ingestor: &'static Ingestor,
) -> Result<(), ConnectorError> {
    let schema = schema_map.get(&req.schema_name).ok_or_else(|| {
        ConnectorError::InitializationError(format!("schema not found: {}", req.schema_name))
    })?;

    let mut seq_no = req.seq_no;
    let records = map_record_batch(req, schema)?;

    for r in records {
        let op = Operation::Insert { new: r };

        ingestor
            .handle_message(IngestionMessage::new_op(0, seq_no as u64, op.clone()))
            .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
        seq_no += 1;
    }

    Ok(())
}

fn map_record_batch(
    req: IngestArrowRequest,
    schema: &Schema,
) -> Result<Vec<Record>, ConnectorError> {
    let mut buf = Bytes::from(req.records).reader();
    // read stream back
    let mut reader = StreamReader::try_new(&mut buf, None).unwrap();
    let mut records = Vec::new();
    while let Some(Ok(batch)) = reader.next() {
        let b_recs = map_record_batch_to_dozer_records(batch, schema)
            .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
        records.extend(b_recs);
    }

    Ok(records)
}
