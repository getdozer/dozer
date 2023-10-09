use std::collections::HashMap;

use dozer_types::{
    arrow::datatypes::Schema as ArrowSchema,
    arrow::{self, ipc::reader::StreamReader},
    arrow_types::{self, from_arrow::map_record_batch_to_dozer_records},
    bytes::{Buf, Bytes},
    grpc_types::ingest::IngestArrowRequest,
    models::ingestion_types::IngestionMessage,
    serde::{Deserialize, Serialize},
    serde_json,
    tonic::async_trait,
    types::{Operation, Record, Schema},
};

use crate::{
    connectors::{CdcType, SourceSchema},
    errors::{ConnectorError, ObjectStoreConnectorError},
    ingestion::Ingestor,
};

use super::{GrpcIngestMessage, IngestAdapter};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct GrpcArrowSchema {
    pub name: String,
    pub schema: arrow::datatypes::Schema,
    #[serde(default)]
    pub cdc_type: CdcType,
}

// Input is a JSON string or a path to a JSON file
// Takes name, arrow schema, and optionally replication type

#[derive(Debug)]
pub struct ArrowAdapter {
    schema_map: HashMap<String, SourceSchema>,
    _arrow_schemas: HashMap<u32, ArrowSchema>,
}

impl ArrowAdapter {
    #[allow(clippy::type_complexity)]
    fn parse_schemas(
        schemas_str: &str,
    ) -> Result<(Vec<(String, SourceSchema)>, HashMap<u32, ArrowSchema>), ConnectorError> {
        let grpc_schemas: Vec<GrpcArrowSchema> =
            serde_json::from_str(schemas_str).map_err(ConnectorError::map_serialization_error)?;
        let mut schemas = vec![];

        let mut arrow_schemas = HashMap::new();

        for (id, grpc_schema) in grpc_schemas.into_iter().enumerate() {
            let schema = arrow_types::from_arrow::map_schema_to_dozer(&grpc_schema.schema)
                .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;

            arrow_schemas.insert(id as u32, grpc_schema.schema);

            schemas.push((
                grpc_schema.name,
                SourceSchema::new(schema, grpc_schema.cdc_type),
            ));
        }
        Ok((schemas, arrow_schemas))
    }
}

#[async_trait]
impl IngestAdapter for ArrowAdapter {
    fn new(schemas_str: String) -> Result<Self, ConnectorError> {
        let (schemas, arrow_schemas) = Self::parse_schemas(&schemas_str)?;
        let schema_map = schemas.into_iter().collect();
        Ok(Self {
            schema_map,
            _arrow_schemas: arrow_schemas,
        })
    }

    fn get_schemas(&self) -> Vec<(String, SourceSchema)> {
        self.schema_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    async fn handle_message(
        &self,
        table_index: usize,
        msg: GrpcIngestMessage,
        ingestor: &'static Ingestor,
    ) -> Result<(), ConnectorError> {
        match msg {
            GrpcIngestMessage::Default(_) => Err(ConnectorError::InitializationError(
                "Wrong message format!".to_string(),
            )),
            GrpcIngestMessage::Arrow(msg) => {
                handle_message(table_index, msg, &self.schema_map, ingestor).await
            }
        }
    }
}

pub async fn handle_message(
    table_index: usize,
    req: IngestArrowRequest,
    schema_map: &HashMap<String, SourceSchema>,
    ingestor: &'static Ingestor,
) -> Result<(), ConnectorError> {
    let schema = &schema_map
        .get(&req.schema_name)
        .ok_or_else(|| {
            ConnectorError::InitializationError(format!("schema not found: {}", req.schema_name))
        })?
        .schema;

    let records = map_record_batch(req, schema)?;

    for r in records {
        let op = Operation::Insert { new: r };

        ingestor
            .handle_message(IngestionMessage::OperationEvent {
                table_index,
                op,
                id: None,
            })
            .await
            .map_err(|_| ConnectorError::IngestorError)?;
    }

    Ok(())
}

fn map_record_batch(
    req: IngestArrowRequest,
    schema: &Schema,
) -> Result<Vec<Record>, ConnectorError> {
    let mut buf = Bytes::from(req.records).reader();
    // read stream back
    let mut reader = StreamReader::try_new(&mut buf, None)?;
    let mut records = Vec::new();
    while let Some(Ok(batch)) = reader.next() {
        let b_recs = map_record_batch_to_dozer_records(batch, schema)
            .map_err(ObjectStoreConnectorError::FromArrowError)?;
        records.extend(b_recs);
    }

    Ok(records)
}
