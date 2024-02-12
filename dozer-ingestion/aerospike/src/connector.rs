use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::event::Event;
use dozer_ingestion_connector::dozer_types::log::{error, info};
use dozer_ingestion_connector::dozer_types::models::connection::AerospikeConnection;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    IngestionMessage, TransactionInfo,
};
use dozer_ingestion_connector::dozer_types::node::{NodeHandle, OpIdentifier, SourceState};
use dozer_ingestion_connector::dozer_types::types::Operation::Insert;
use dozer_ingestion_connector::dozer_types::types::{Field, FieldDefinition, FieldType, Schema};
use dozer_ingestion_connector::tokio::sync::broadcast::error::RecvError;
use dozer_ingestion_connector::tokio::sync::broadcast::Receiver;
use dozer_ingestion_connector::tokio::sync::{mpsc, oneshot};
use dozer_ingestion_connector::{
    async_trait, dozer_types, tokio, Connector, Ingestor, SourceSchema, SourceSchemaResult,
    TableIdentifier, TableInfo,
};
use std::collections::HashMap;
use std::ffi::CString;
use std::num::TryFromIntError;

use dozer_ingestion_connector::dozer_types::serde::Deserialize;

use actix_web::dev::Server;
use actix_web::post;
use actix_web::web;
use actix_web::App;
use actix_web::HttpRequest;
use actix_web::HttpServer;
use actix_web::{get, HttpResponse};

use dozer_ingestion_connector::dozer_types::ordered_float::OrderedFloat;
use dozer_ingestion_connector::dozer_types::prost::Message;
use dozer_ingestion_connector::dozer_types::rust_decimal::Decimal;
use dozer_ingestion_connector::dozer_types::serde_json;
use dozer_ingestion_connector::dozer_types::serde_json::Value;

use base64::prelude::*;
use dozer_ingestion_connector::dozer_types::chrono::{
    DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc,
};
use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};
use dozer_ingestion_connector::schema_parser::SchemaParser;

use dozer_sink_aerospike::Client;

#[derive(Debug, Error)]
pub enum AerospikeConnectorError {
    #[error("Cannot start server: {0}")]
    CannotStartServer(#[from] std::io::Error),

    #[error("Set name is none. Key: {0:?}, {1:?}, {2:?}")]
    SetNameIsNone(Option<String>, Option<String>, Option<String>),

    #[error("PK is none: {0:?}, {1:?}, {2:?}")]
    PkIsNone(Option<String>, String, Option<String>),

    #[error("Invalid key value: {0:?}. Key is supposed to have 4 elements.")]
    InvalidKeyValue(Vec<Option<String>>),

    #[error("Unsupported type. Bin type {bin_type:?}, field type: {field_type:?}")]
    UnsupportedTypeForFieldType {
        bin_type: String,
        field_type: FieldType,
    },

    #[error("Unsupported type: {0}")]
    UnsupportedType(FieldType),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),

    #[error("Invalid days: {0}")]
    InvalidDate(i64),

    #[error("Error decoding base64: {0}")]
    BytesDecodingError(#[from] base64::DecodeError),

    #[error("Error parsing float: {0}")]
    FloatParsingError(#[from] std::num::ParseFloatError),

    #[error("Error parsing int: {0}")]
    IntParsingError(#[from] std::num::ParseIntError),

    #[error("Error casting int: {0}")]
    IntCastError(#[from] TryFromIntError),

    #[error("Failed days number parsing")]
    ParsingDaysError,

    #[error("Failed timestamp parsing")]
    ParsingTimestampFailed,

    #[error("Failed int parsing")]
    ParsingIntFailed,

    #[error("Failed uint parsing")]
    ParsingUIntFailed,

    #[error("Failed float parsing")]
    ParsingFloatFailed,

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Failed parsing timestamp: {0}")]
    TimestampParsingError(#[from] dozer_ingestion_connector::dozer_types::chrono::ParseError),
}

#[derive(Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct AerospikeEvent {
    msg: String,
    key: Vec<Option<String>>,
    // gen: u32,
    // exp: u32,
    lut: u64,
    bins: Vec<Bin>,
}

#[derive(Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct Bin {
    name: String,
    value: Option<dozer_types::serde_json::Value>,
    r#type: String,
}

#[derive(Debug)]
pub struct AerospikeConnector {
    pub config: AerospikeConnection,
    node_handle: NodeHandle,
    event_receiver: Receiver<Event>,
}

impl AerospikeConnector {
    pub fn new(
        config: AerospikeConnection,
        node_handle: NodeHandle,
        event_receiver: Receiver<Event>,
    ) -> Self {
        Self {
            config,
            node_handle,
            event_receiver,
        }
    }

    fn start_server(&self, server_state: ServerState) -> Result<Server, AerospikeConnectorError> {
        let address = format!(
            "{}:{}",
            self.config.replication.server_address, self.config.replication.server_port
        );

        info!("Starting aerospike replication server on {}", address);

        Ok(HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(server_state.clone()))
                .service(healthcheck)
                .service(event_request_handler)
        })
        .bind(address)?
        .run())
    }
}

#[derive(Debug)]
struct PendingMessage {
    message: IngestionMessage,
    sender: oneshot::Sender<()>,
}

#[derive(Debug)]
struct PendingOperationId {
    operation_id: u64,
    sender: oneshot::Sender<()>,
}

/// This loop assigns an operation id to each request and sends it to the ingestor.
async fn ingestor_loop(
    mut message_receiver: mpsc::UnboundedReceiver<PendingMessage>,
    ingestor: Ingestor,
    operation_id_sender: mpsc::UnboundedSender<PendingOperationId>,
) {
    let mut operation_id = 0;
    while let Some(message) = message_receiver.recv().await {
        let pending_operation_id = PendingOperationId {
            operation_id,
            sender: message.sender,
        };

        // Propagate panic in the pipeline event processor loop.
        operation_id_sender.send(pending_operation_id).unwrap();

        // Ignore the error, because the server can be down.
        let _ = ingestor.handle_message(message.message).await;
        let _ = ingestor
            .handle_message(IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                id: Some(OpIdentifier::new(0, operation_id)),
            }))
            .await;

        operation_id += 1;
    }
}

/// This loop triggers the pending operation id that's before the event's payload.
async fn pipeline_event_processor(
    node_handle: NodeHandle,
    mut operation_id_receiver: mpsc::UnboundedReceiver<PendingOperationId>,
    mut event_receiver: Receiver<Event>,
) {
    let mut operation_id_from_pipeline = None;
    let mut pending_operation_id: Option<PendingOperationId> = None;
    loop {
        if operation_id_from_pipeline
            < pending_operation_id
                .as_ref()
                .map(|operation_id| operation_id.operation_id)
        {
            // We have pending operation id, wait for pipeline event.
            let event = match event_receiver.recv().await {
                Ok(event) => event,
                Err(RecvError::Closed) => {
                    // Pipeline is down.
                    return;
                }
                Err(RecvError::Lagged(_)) => {
                    // Ignore lagged events.
                    continue;
                }
            };
            if let Some(operation_id) = get_operation_id_from_event(&event, &node_handle) {
                operation_id_from_pipeline = Some(operation_id);
            }
        } else if let Some(pending) = pending_operation_id.take() {
            // This operation id is already confirmed by the pipeline.
            let _ = pending.sender.send(());
        } else {
            // Wait for the next operation id.
            let Some(pending) = operation_id_receiver.recv().await else {
                // Ingestor is down.
                return;
            };
            pending_operation_id = Some(pending);
        }
    }
}

fn get_operation_id_from_event(event: &Event, node_handle: &NodeHandle) -> Option<u64> {
    match event {
        Event::SinkFlushed { epoch, .. } => epoch
            .common_info
            .source_states
            .get(node_handle)
            .and_then(|state| match state {
                SourceState::Restartable(id) => Some(id.seq_in_tx),
                _ => None,
            }),
    }
}

fn map_error(error: AerospikeConnectorError) -> HttpResponse {
    error!("Aerospike ingestion error: {:?}", error);
    HttpResponse::InternalServerError().finish()
}

#[get("/")]
async fn healthcheck(_req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[post("/")]
async fn event_request_handler(
    json: web::Json<AerospikeEvent>,
    data: web::Data<ServerState>,
) -> HttpResponse {
    let event = json.into_inner();
    let state = data.into_inner();

    // TODO: Handle delete
    if event.msg != "write" {
        return HttpResponse::Ok().finish();
    }

    let operation_events = map_events(event, &state.tables_index_map).await;

    match operation_events {
        Ok(None) => HttpResponse::Ok().finish(),
        Ok(Some(message)) => {
            let (sender, receiver) = oneshot::channel::<()>();
            if let Err(e) = state.sender.send(PendingMessage { message, sender }) {
                error!("Ingestor is down: {:?}", e);
                return HttpResponse::InternalServerError().finish();
            }
            if let Err(e) = receiver.await {
                error!("Pipeline event processor is down: {:?}", e);
                HttpResponse::InternalServerError().finish()
            } else {
                HttpResponse::Ok().finish()
            }
        }
        Err(e) => map_error(e),
    }
}

#[derive(Clone, Debug)]
struct TableIndexMap {
    table_index: usize,
    columns_map: HashMap<String, (usize, FieldType)>,
}

#[derive(Clone)]
struct ServerState {
    tables_index_map: HashMap<String, TableIndexMap>,
    sender: mpsc::UnboundedSender<PendingMessage>,
}

#[async_trait]
impl Connector for AerospikeConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        vec![
            ("str".into(), Some(FieldType::Decimal)),
            ("bool".into(), Some(FieldType::Boolean)),
            ("int".into(), Some(FieldType::Int)),
            ("float".into(), Some(FieldType::Float)),
            ("blob".into(), Some(FieldType::Boolean)),
            ("list".into(), None),
            ("map".into(), None),
            ("geojson".into(), None),
        ]
    }

    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(self
            .config
            .sets
            .iter()
            .map(|set| TableIdentifier {
                schema: Some(self.config.namespace.clone()),
                name: set.to_string(),
            })
            .collect())
    }

    async fn validate_tables(&mut self, _tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn list_columns(
        &mut self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        Ok(vec![])
    }

    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let schemas: HashMap<String, SourceSchema> = match self.config.schemas.clone() {
            Some(schemas) => {
                let schema = SchemaParser::parse_config(&schemas)?;
                serde_json::from_str(&schema)?
            }
            None => table_infos
                .iter()
                .map(|table_info| {
                    let table_name = table_info.name.clone();
                    let primary_index = table_info
                        .column_names
                        .iter()
                        .position(|n| n == "PK")
                        .map_or(vec![], |i| vec![i]);

                    (
                        table_name,
                        SourceSchema {
                            schema: Schema {
                                fields: table_info
                                    .column_names
                                    .iter()
                                    .map(|name| FieldDefinition {
                                        name: name.clone(),
                                        typ: if name == "inserted_at" {
                                            FieldType::Timestamp
                                        } else {
                                            FieldType::String
                                        },
                                        nullable: true,
                                        source: Default::default(),
                                    })
                                    .collect(),
                                primary_index,
                            },
                            cdc_type: Default::default(),
                        },
                    )
                })
                .collect(),
        };

        Ok(table_infos
            .iter()
            .map(|table_info| {
                let table_name = table_info.name.clone();
                let schema = schemas
                    .get(&table_name)
                    .cloned()
                    .ok_or(AerospikeConnectorError::SchemaNotFound(table_name.clone()))?;

                let filtered_schema = if table_info.column_names.is_empty() {
                    schema
                } else {
                    let primary_key_field_names: Vec<String> = schema
                        .schema
                        .primary_index
                        .iter()
                        .map(|idx| {
                            schema
                                .schema
                                .fields
                                .get(*idx)
                                .map(|field| field.name.clone())
                                .expect("Field should be present")
                        })
                        .collect();

                    let filtered_fields: Vec<FieldDefinition> = schema
                        .schema
                        .fields
                        .into_iter()
                        .filter(|field| table_info.column_names.contains(&field.name))
                        .collect();

                    let new_primary_index = filtered_fields
                        .iter()
                        .enumerate()
                        .filter_map(|(i, field)| {
                            if primary_key_field_names.contains(&field.name) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();

                    SourceSchema {
                        schema: Schema {
                            fields: filtered_fields,
                            primary_index: new_primary_index,
                        },
                        cdc_type: Default::default(),
                    }
                };

                Ok(filtered_schema)
            })
            .collect())
    }

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let hosts = CString::new(self.config.hosts.as_str())?;
        let client = Client::new(&hosts).map_err(Box::new)?;
        unsafe {
            let mut response: *mut i8 = std::ptr::null_mut();
            let request = CString::new("info")?;
            client.info(&request, &mut response).map_err(Box::new)?;
        }

        let mapped_schema = self.get_schemas(&tables).await?;
        ingestor
            .handle_message(IngestionMessage::TransactionInfo(
                TransactionInfo::SnapshottingStarted,
            ))
            .await?;
        ingestor
            .handle_message(IngestionMessage::TransactionInfo(
                TransactionInfo::SnapshottingDone { id: None },
            ))
            .await?;

        let tables_index_map: HashMap<String, TableIndexMap> = mapped_schema
            .into_iter()
            .enumerate()
            .map(|(table_index, schema)| {
                let columns_map: HashMap<String, (usize, FieldType)> = schema
                    .expect("Schema should be present")
                    .schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| (field.name.clone(), (i, field.typ)))
                    .collect();

                (
                    tables[table_index].name.clone(),
                    TableIndexMap {
                        table_index,
                        columns_map,
                    },
                )
            })
            .collect();

        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        let (operation_id_sender, operation_id_receiver) = mpsc::unbounded_channel();
        let ingestor = ingestor.clone();
        tokio::spawn(async move {
            ingestor_loop(message_receiver, ingestor, operation_id_sender).await
        });
        let node_handle = self.node_handle.clone();
        let event_receiver = self.event_receiver.resubscribe();
        tokio::spawn(async move {
            pipeline_event_processor(node_handle, operation_id_receiver, event_receiver).await
        });
        let server_state = ServerState {
            tables_index_map: tables_index_map.clone(),
            sender: message_sender,
        };

        let _server = self.start_server(server_state)?.await;

        Ok(())
    }
}

async fn map_events(
    event: AerospikeEvent,
    tables_map: &HashMap<String, TableIndexMap>,
) -> Result<Option<IngestionMessage>, AerospikeConnectorError> {
    let key: [Option<String>; 4] = match event.key.try_into() {
        Ok(key) => key,
        Err(key) => return Err(AerospikeConnectorError::InvalidKeyValue(key)),
    };
    let [key0, set_name, key2, pk_in_key] = key;
    let Some(set_name) = set_name else {
        return Err(AerospikeConnectorError::SetNameIsNone(
            key0, key2, pk_in_key,
        ));
    };

    let Some(TableIndexMap {
        columns_map,
        table_index,
    }) = tables_map.get(set_name.as_str())
    else {
        return Ok(None);
    };

    let mut fields = vec![Field::Null; columns_map.len()];
    if let Some((pk, _)) = columns_map.get("PK") {
        if let Some(pk_in_key) = pk_in_key {
            fields[*pk] = Field::String(pk_in_key);
        } else {
            return Err(AerospikeConnectorError::PkIsNone(key0, set_name, key2));
        }
    }

    if let Some((index, _)) = columns_map.get("inserted_at") {
        // Create a NaiveDateTime from the timestamp
        let naive = NaiveDateTime::from_timestamp_millis(event.lut as i64)
            .ok_or(AerospikeConnectorError::InvalidTimestamp(event.lut as i64))?;

        // Create a normal DateTime from the NaiveDateTime
        let datetime: DateTime<FixedOffset> =
            DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).fixed_offset();

        fields[*index] = Field::Timestamp(datetime);
    }

    for bin in event.bins {
        if let Some((i, typ)) = columns_map.get(bin.name.as_str()) {
            fields[*i] = match bin.value {
                Some(value) => map_value_to_field(bin.r#type.as_str(), value, *typ)?,
                None => Field::Null,
            };
        }
    }

    Ok(Some(IngestionMessage::OperationEvent {
        table_index: *table_index,
        op: Insert {
            new: dozer_types::types::Record::new(fields),
        },
        id: None,
    }))
}

pub(crate) fn map_value_to_field(
    bin_type: &str,
    value: Value,
    typ: FieldType,
) -> Result<Field, AerospikeConnectorError> {
    match value {
        Value::Null => Ok(Field::Null),
        Value::Bool(b) => match typ {
            FieldType::UInt => Ok(Field::UInt(b as u64)),
            FieldType::U128 => Ok(Field::U128(b as u128)),
            FieldType::Int => Ok(Field::Int(b as i64)),
            FieldType::I128 => Ok(Field::I128(b as i128)),
            FieldType::Float => Ok(Field::Float(OrderedFloat(if b { 1.0 } else { 0.0 }))),
            FieldType::Boolean => Ok(Field::Boolean(b)),
            FieldType::String => Ok(Field::String(b.to_string())),
            FieldType::Text => Ok(Field::Text(b.to_string())),
            FieldType::Binary => Ok(Field::Binary(b.encode_to_vec())),
            FieldType::Decimal => Ok(Field::Decimal(Decimal::from(b as i8))),
            typ => Err(AerospikeConnectorError::UnsupportedType(typ)),
        },
        Value::Number(v) => {
            match typ {
                FieldType::UInt => Ok(Field::UInt(
                    v.as_u64()
                        .ok_or(AerospikeConnectorError::ParsingUIntFailed)?,
                )),
                FieldType::U128 => Ok(Field::U128(
                    v.as_u64()
                        .ok_or(AerospikeConnectorError::ParsingUIntFailed)?
                        as u128,
                )),
                FieldType::Int => Ok(Field::Int(
                    v.as_i64()
                        .ok_or(AerospikeConnectorError::ParsingIntFailed)?,
                )),
                FieldType::I128 => Ok(Field::I128(
                    v.as_i64()
                        .ok_or(AerospikeConnectorError::ParsingIntFailed)?
                        as i128,
                )),
                FieldType::Float => Ok(Field::Float(OrderedFloat(
                    v.as_f64()
                        .ok_or(AerospikeConnectorError::ParsingFloatFailed)?,
                ))),
                FieldType::Boolean => Ok(Field::Boolean(
                    v.as_i64()
                        .ok_or(AerospikeConnectorError::ParsingIntFailed)?
                        == 1,
                )),
                FieldType::String => Ok(Field::String(v.to_string())),
                FieldType::Text => Ok(Field::Text(v.to_string())),
                FieldType::Binary => Ok(Field::Binary(v.to_string().as_bytes().to_vec())),
                FieldType::Timestamp => {
                    // TODO: decide on the format of the timestamp

                    // Convert the timestamp string into an i64
                    let timestamp = v
                        .as_i64()
                        .ok_or(AerospikeConnectorError::ParsingTimestampFailed)?;

                    // Create a NaiveDateTime from the timestamp
                    let naive = NaiveDateTime::from_timestamp_opt(timestamp, 0)
                        .ok_or(AerospikeConnectorError::InvalidTimestamp(timestamp))?;

                    // Create a normal DateTime from the NaiveDateTime
                    let datetime: DateTime<FixedOffset> =
                        DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).fixed_offset();
                    Ok(Field::Timestamp(datetime))
                }
                FieldType::Date => {
                    let days = v
                        .as_i64()
                        .ok_or(AerospikeConnectorError::ParsingDaysError)?;

                    let date = NaiveDate::from_num_days_from_ce_opt(days.try_into()?)
                        .ok_or(AerospikeConnectorError::InvalidDate(days))?;
                    Ok(Field::Date(date))
                }
                typ => Err(AerospikeConnectorError::UnsupportedType(typ)),
            }
        }
        Value::String(s) => {
            match typ {
                FieldType::UInt => Ok(Field::UInt(s.as_str().parse()?)),
                FieldType::U128 => Ok(Field::U128(s.as_str().parse()?)),
                FieldType::Int => Ok(Field::Int(s.as_str().parse()?)),
                FieldType::I128 => Ok(Field::I128(s.as_str().parse()?)),
                FieldType::Float => Ok(Field::Float(OrderedFloat(s.parse()?))),
                FieldType::Boolean => Ok(Field::Boolean(s == "true" || s == "1")),
                FieldType::String => Ok(Field::String(s)),
                FieldType::Text => Ok(Field::Text(s)),
                FieldType::Timestamp => Ok(Field::Timestamp(DateTime::parse_from_rfc3339(&s)?)),
                FieldType::Date => {
                    // TODO: decide on the format of the date

                    Err(AerospikeConnectorError::UnsupportedType(typ))
                }
                FieldType::Binary => {
                    let bytes = BASE64_STANDARD.decode(s.as_bytes())?;
                    Ok(Field::Binary(bytes))
                }
                typ => Err(AerospikeConnectorError::UnsupportedType(typ)),
            }
        }
        Value::Object(_) | Value::Array(_) => {
            Err(AerospikeConnectorError::UnsupportedTypeForFieldType {
                bin_type: bin_type.to_string(),
                field_type: typ,
            })
        }
    }
}
