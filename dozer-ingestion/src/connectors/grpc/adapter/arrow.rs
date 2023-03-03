use std::marker::PhantomData;

use dozer_types::{
    arrow::ipc::reader::StreamReader,
    arrow_types::{self, from_arrow::map_record_batch_to_dozer_records},
    bytes::{Buf, Bytes},
    grpc_types::ingest::IngestArrowRequest,
    serde_json,
    types::{Record, ReplicationChangesTrackingType, Schema, SchemaIdentifier, SourceSchema},
};

use crate::errors::ConnectorError;
use dozer_types::serde::{self, Deserialize, Serialize};

use super::GrpcIngestAdapter;

#[derive(Debug)]
pub struct ArrowAdapter<T> {
    arrow_schemas: Vec<Schema>,
    phantom: PhantomData<T>,
}

impl ArrowAdapter<IngestArrowRequest> {
    pub fn new(arrow_schemas: Vec<Schema>) -> Self {
        Self {
            arrow_schemas,
            phantom: PhantomData,
        }
    }

    fn map_record_batch(
        req: IngestArrowRequest,
        schema: &Schema,
    ) -> Result<Vec<Record>, tonic::Status> {
        let mut buf = Bytes::from(req.records).reader();
        // read stream back
        let mut reader = StreamReader::try_new(&mut buf, None).unwrap();
        let mut records = Vec::new();
        while let Some(Ok(batch)) = reader.next() {
            let b_recs = map_record_batch_to_dozer_records(batch, schema).map_err(|e| {
                tonic::Status::internal(format!(
                    "error converting arrow record batch to dozer records: {:#?}",
                    e
                ))
            })?;
            records.extend(b_recs);
        }

        Ok(records)
    }
}

impl GrpcIngestAdapter<IngestArrowRequest> for ArrowAdapter<IngestArrowRequest> {
    fn get_schemas(&self, schemas_str: &String) -> Result<Vec<SourceSchema>, ConnectorError> {
        let grpc_schemas: Vec<GrpcArrowSchema> =
            serde_json::from_str(&schemas_str).map_err(ConnectorError::map_serialization_error)?;
        let mut schemas = vec![];

        for (id, grpc_schema) in grpc_schemas.iter().enumerate() {
            let mut schema = arrow_types::from_arrow::map_schema_to_dozer(&grpc_schema.schema)
                .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
            schema.identifier = Some(SchemaIdentifier {
                id: id as u32,
                version: 1,
            });

            schemas.push(SourceSchema {
                name: grpc_schema.name.clone(),
                schema,
                replication_type: grpc_schema.replication_type.clone(),
            });
        }
        Ok(schemas)
    }

    fn handle_message(&self, msg: IngestArrowRequest) -> Result<(), ConnectorError> {
        todo!()
    }
}

// Input is a JSON string or a path to a JSON file
// Takes name, arrow schema, and optionally replication type

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(crate = "self::serde")]
pub struct GrpcArrowSchema {
    pub name: String,
    pub schema: dozer_types::arrow::datatypes::Schema,
    #[serde(default)]
    pub replication_type: ReplicationChangesTrackingType,
}
