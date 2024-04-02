use dozer_ingestion_connector::dozer_types::epoch::SourceTime;
use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::errors::types::DeserializationError;
use dozer_ingestion_connector::dozer_types::event::Event;
use dozer_ingestion_connector::dozer_types::json_types::serde_json_to_json_value;
use dozer_ingestion_connector::dozer_types::log::{debug, error, info, trace, warn};
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
use std::ffi::{CStr, CString};
use std::num::TryFromIntError;

use std::time::Duration;

use dozer_ingestion_connector::dozer_types::serde::Deserialize;

use actix_web::dev::Server;
use actix_web::post;
use actix_web::web;
use actix_web::App;
use actix_web::HttpRequest;
use actix_web::HttpServer;
use actix_web::{get, HttpResponse};

use dozer_ingestion_connector::dozer_types::rust_decimal::Decimal;
use dozer_ingestion_connector::dozer_types::serde_json;
use dozer_ingestion_connector::dozer_types::serde_json::Value;

use base64::prelude::*;
use dozer_ingestion_connector::dozer_types::chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};

use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};
use dozer_ingestion_connector::schema_parser::SchemaParser;

use dozer_sink_aerospike::Client;

#[derive(Debug, Error)]
pub enum AerospikeConnectorError {
    #[error("Cannot start server: {0}")]
    CannotStartServer(#[from] std::io::Error),

    #[error("Set name is none. Key: {0:?}, {1:?}, {2:?}")]
    SetNameIsNone(
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
    ),

    #[error("PK is none: {0:?}, {1:?}, {2:?}")]
    PkIsNone(Option<serde_json::Value>, String, Option<serde_json::Value>),

    #[error("Invalid key value: {0:?}. Key is supposed to have 4 elements.")]
    InvalidKeyValue(Vec<Option<serde_json::Value>>),

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

    #[error("Failed point parsing")]
    ParsingPointFailed,

    #[error("Failed int parsing")]
    ParsingIntFailed,

    #[error("Failed uint parsing")]
    ParsingUIntFailed,

    #[error("Failed float parsing")]
    ParsingFloatFailed,

    #[error("Failed decimal parsing")]
    ParsingDecimalFailed(#[from] dozer_types::rust_decimal::Error),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Failed parsing timestamp: {0}")]
    TimestampParsingError(#[from] dozer_ingestion_connector::dozer_types::chrono::ParseError),

    #[error("Key is neither string or int")]
    KeyNotSupported(Value),

    #[error("Failed to parse json")]
    JsonParsingFailed(#[from] DeserializationError),

    #[error("Failed to parse duration")]
    ParsingDurationFailed,
}

#[derive(Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct AerospikeEvent {
    msg: String,
    key: Vec<Option<serde_json::Value>>,
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
                .app_data(web::JsonConfig::default().error_handler(|err, _req| {
                    error!("Error parsing json: {:?}", err);
                    actix_web::error::InternalError::from_response(
                        "",
                        HttpResponse::BadRequest()
                            .content_type("application/json")
                            .body(format!(r#"{{"error":"{}"}}"#, err)),
                    )
                    .into()
                }))
                .app_data(web::Data::new(server_state.clone()))
                .service(healthcheck)
                .service(healthcheck_batch)
                .service(event_request_handler)
                .service(batch_event_request_handler)
        })
        .bind(address)?
        .run())
    }

    async fn rewind(
        &self,
        client: &Client,
        dc_name: &str,
        namespace: &str,
    ) -> Result<bool, BoxedError> {
        unsafe {
            let request = CString::new(format!(
                "set-config:context=xdr;dc={dc_name};namespace={namespace};action=add;rewind=all"
            ))?;

            // Wait until the replication configuration is set.
            // It may take some time, so retrying until rewind returns ok.
            let mut response: *mut i8 = std::ptr::null_mut();
            client.info(&request, &mut response).map_err(Box::new)?;

            let string = CStr::from_ptr(response);

            let parts: Vec<&str> = string.to_str()?.trim().split('\t').collect();

            if let Some(status) = parts.get(1) {
                Ok(status.replace('\n', "") == *"ok")
            } else {
                Ok(false)
            }
        }
    }
}

#[derive(Debug)]
struct PendingMessage {
    source_time: SourceTime,
    messages: Vec<IngestionMessage>,
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
        for message in message.messages {
            let _ = ingestor.handle_message(message).await;
        }
        let _ = ingestor
            .handle_message(IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                id: Some(OpIdentifier::new(0, operation_id)),
                source_time: Some(message.source_time),
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

#[get("/batch")]
async fn healthcheck_batch(_req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[post("/")]
async fn event_request_handler(
    json: web::Json<AerospikeEvent>,
    data: web::Data<ServerState>,
) -> HttpResponse {
    let event = json.into_inner();
    let state = data.into_inner();

    trace!(target: "aerospike_http_server", "Event data: {:?}", event);
    // TODO: Handle delete
    if event.msg != "write" {
        return HttpResponse::Ok().finish();
    }

    let source_time = SourceTime::new(event.lut, 1);
    let message = map_record(event, &state.tables_index_map);

    trace!(target: "aerospike_http_server", "Mapped message {:?}", message);
    match message {
        Ok(None) => HttpResponse::Ok().finish(),
        Ok(Some(message)) => {
            let (sender, receiver) = oneshot::channel::<()>();
            if let Err(e) = state.sender.send(PendingMessage {
                source_time,
                messages: vec![message],
                sender,
            }) {
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

#[post("/batch")]
async fn batch_event_request_handler(
    json: web::Json<Vec<AerospikeEvent>>,
    data: web::Data<ServerState>,
) -> HttpResponse {
    let events = json.into_inner();
    let state = data.into_inner();

    debug!(target: "aerospike_http_server", "Aerospike events count {:?}", events.len());
    trace!(target: "aerospike_http_server", "Aerospike events {:?}", events);

    let mut min_lut = u64::MAX;
    let messages = match events
        .into_iter()
        .filter_map(|e| {
            let lut = e.lut;
            let msg = map_record(e, &state.tables_index_map).transpose()?;
            min_lut = min_lut.min(lut);
            Some(msg)
        })
        .collect::<Result<Vec<_>, AerospikeConnectorError>>()
    {
        Ok(msgs) => msgs,
        Err(e) => return map_error(e),
    };

    debug!(target: "aerospike_http_server", "Mapped {:?} messages", messages.len());
    trace!(target: "aerospike_http_server", "Mapped messages {:?}", messages);

    if !messages.is_empty() {
        let (sender, receiver) = oneshot::channel::<()>();
        if let Err(e) = state.sender.send(PendingMessage {
            messages,
            sender,
            source_time: SourceTime::new(min_lut, 1),
        }) {
            error!("Ingestor is down: {:?}", e);
            return HttpResponse::InternalServerError().finish();
        }

        if let Err(e) = receiver.await {
            error!("Pipeline event processor is down: {:?}", e);
            return HttpResponse::InternalServerError().finish();
        }
    }

    HttpResponse::Ok().finish()
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
                                        } else if name == "PK" {
                                            FieldType::UInt
                                        } else {
                                            FieldType::String
                                        },
                                        nullable: name != "PK",
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
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let hosts = CString::new(self.config.hosts.as_str())?;
        let client = Client::new(&hosts).map_err(Box::new)?;

        if last_checkpoint.is_none() {
            let dc_name = self.config.replication.datacenter.clone();
            let namespace = self.config.namespace.clone();

            // To read data snapshot we need to rewind xdr stream.
            // Before rewinding we need to remove xdr configuration and then add it again.
            unsafe {
                let request = CString::new(format!(
                    "set-config:context=xdr;dc={dc_name};namespace={namespace};action=remove"
                ))?;
                let mut response: *mut i8 = std::ptr::null_mut();
                client.info(&request, &mut response).map_err(Box::new)?;
            }

            loop {
                if self.rewind(&client, &dc_name, &namespace).await? {
                    info!("Aerospike replication configuration set successfully");
                    break;
                } else {
                    warn!("Aerospike replication configuration set failed");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
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

fn map_record(
    event: AerospikeEvent,
    tables_map: &HashMap<String, TableIndexMap>,
) -> Result<Option<IngestionMessage>, AerospikeConnectorError> {
    let key: [Option<serde_json::Value>; 4] = match event.key.try_into() {
        Ok(key) => key,
        Err(key) => return Err(AerospikeConnectorError::InvalidKeyValue(key)),
    };
    let [key0, set_name, key2, pk_in_key] = key;
    let Some(set_name) = set_name else {
        return Err(AerospikeConnectorError::SetNameIsNone(
            key0, key2, pk_in_key,
        ));
    };

    let table_name = match set_name {
        serde_json::Value::String(s) => s.clone(),
        _ => {
            return Err(AerospikeConnectorError::SetNameIsNone(
                key0, key2, pk_in_key,
            ))
        }
    };

    let Some(TableIndexMap {
        columns_map,
        table_index,
    }) = tables_map.get(&table_name)
    else {
        return Ok(None);
    };

    let mut fields = vec![Field::Null; columns_map.len()];
    if let Some((pk, _)) = columns_map.get("PK") {
        if let Some(pk_in_key) = pk_in_key {
            match pk_in_key {
                serde_json::Value::String(s) => {
                    fields[*pk] = Field::String(s.clone());
                }
                serde_json::Value::Number(n) => {
                    fields[*pk] = Field::UInt(
                        n.as_u64()
                            .ok_or(AerospikeConnectorError::ParsingUIntFailed)?,
                    );
                }
                v => return Err(AerospikeConnectorError::KeyNotSupported(v)),
            }
        } else {
            return Err(AerospikeConnectorError::PkIsNone(key0, table_name, key2));
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
        id: Some(OpIdentifier::new(event.lut, 0)),
    }))
}

pub(crate) fn map_value_to_field(
    bin_type: &str,
    mut value: Value,
    typ: FieldType,
) -> Result<Field, AerospikeConnectorError> {
    if value.is_null() {
        return Ok(Field::Null);
    }
    let unsupported_type = || AerospikeConnectorError::UnsupportedTypeForFieldType {
        bin_type: bin_type.to_owned(),
        field_type: typ,
    };
    let check_type = |wanted_typ| {
        if bin_type == wanted_typ {
            Ok(())
        } else {
            Err(unsupported_type())
        }
    };
    match typ {
        FieldType::UInt => {
            check_type("int")?;
            let number = value.as_number().ok_or_else(unsupported_type)?;
            Ok(Field::UInt(number.as_u64().ok_or_else(|| {
                AerospikeConnectorError::ParsingUIntFailed
            })?))
        }
        FieldType::Int => {
            check_type("int")?;
            let number = value.as_number().ok_or_else(unsupported_type)?;
            Ok(Field::Int(number.as_i64().ok_or_else(|| {
                AerospikeConnectorError::ParsingIntFailed
            })?))
        }
        FieldType::U128 => {
            check_type("str")?;
            let string = value.as_str().ok_or_else(unsupported_type)?;
            Ok(Field::U128(string.parse()?))
        }
        FieldType::I128 => {
            check_type("str")?;
            let string = value.as_str().ok_or_else(unsupported_type)?;
            Ok(Field::I128(string.parse()?))
        }
        FieldType::Float => {
            check_type("float")?;
            let number = value.as_number().ok_or_else(unsupported_type)?;
            Ok(Field::Float(
                number
                    .as_f64()
                    .ok_or(AerospikeConnectorError::ParsingFloatFailed)?
                    .into(),
            ))
        }
        FieldType::Boolean => {
            check_type("bool")?;
            Ok(Field::Boolean(
                value.as_bool().ok_or_else(unsupported_type)?,
            ))
        }
        FieldType::String => {
            check_type("str")?;
            Ok(Field::String(
                value.as_str().ok_or_else(unsupported_type)?.to_owned(),
            ))
        }
        FieldType::Text => {
            check_type("str")?;
            Ok(Field::Text(
                value.as_str().ok_or_else(unsupported_type)?.to_owned(),
            ))
        }
        FieldType::Binary => {
            check_type("blob")?;
            if bin_type != "blob" {
                return Err(unsupported_type());
            }
            let string = value.as_str().ok_or_else(unsupported_type)?;
            Ok(Field::Binary(BASE64_STANDARD.decode(string)?))
        }
        FieldType::Decimal => {
            check_type("str")?;
            let string = value.as_str().ok_or_else(unsupported_type)?;
            Ok(Field::Decimal(string.parse()?))
        }
        FieldType::Timestamp => {
            check_type("str")?;
            let string = value.as_str().ok_or_else(unsupported_type)?;
            Ok(Field::Timestamp(DateTime::parse_from_rfc3339(string)?))
        }
        FieldType::Date => {
            check_type("str")?;
            let string = value.as_str().ok_or_else(unsupported_type)?;
            Ok(Field::Date(string.parse()?))
        }
        FieldType::Json => Ok(Field::Json(serde_json_to_json_value(value)?)),
        FieldType::Point => {
            check_type("geojson")?;
            let json = value.as_object_mut().ok_or_else(unsupported_type)?;
            if !json.get("type").is_some_and(|type_| type_ == "Point") {
                return Err(AerospikeConnectorError::ParsingPointFailed);
            }
            let Some(Value::Array(coords)) = json.remove("coordinates") else {
                return Err(AerospikeConnectorError::ParsingPointFailed);
            };
            let p: [Value; 2] = coords
                .try_into()
                .map_err(|_| AerospikeConnectorError::ParsingPointFailed)?;
            if let (Some(x), Some(y)) = (p[0].as_f64(), p[1].as_f64()) {
                Ok(Field::Point((x, y).into()))
            } else {
                Err(AerospikeConnectorError::ParsingPointFailed)
            }
        }
        FieldType::Duration => {
            check_type("str")?;
            let string = value.as_str().ok_or_else(unsupported_type)?;
            let duration = parse_duration(string)?;
            Ok(Field::Duration(dozer_types::types::DozerDuration(
                duration,
                dozer_types::types::TimeUnit::Nanoseconds,
            )))
        }
    }
}

fn parse_duration(string: &str) -> Result<Duration, AerospikeConnectorError> {
    let err = |_| AerospikeConnectorError::ParsingDurationFailed;
    if !string.get(0..2).is_some_and(|chars| chars == "PT") {
        return Err(AerospikeConnectorError::ParsingDurationFailed);
    }
    let string = &string[2..];
    let to_duration = |scale, number: &Decimal| -> Result<Duration, AerospikeConnectorError> {
        let as_secs: Decimal = number * Decimal::new(scale, 0);
        let secs = as_secs.try_into().map_err(err)?;
        let frac = as_secs.fract() * Decimal::new(1_000_000_000, 0);
        Ok(Duration::new(secs, frac.try_into().map_err(err)?))
    };
    let (hours, string) = parse_duration_part(string, 'H')?;
    let mut duration = to_duration(3600, &hours)?;
    if hours.is_integer() {
        let (mins, string) = parse_duration_part(string, 'M')?;
        duration += to_duration(60, &mins)?;
        if mins.is_integer() {
            let (secs, string) = parse_duration_part(string, 'S')?;
            duration += to_duration(1, &secs)?;
            if !string.is_empty() {
                return Err(AerospikeConnectorError::ParsingDurationFailed);
            }
        } else if !string.is_empty() {
            return Err(AerospikeConnectorError::ParsingDurationFailed);
        }
    } else if !string.is_empty() {
        return Err(AerospikeConnectorError::ParsingDurationFailed);
    }
    Ok(duration)
}

fn parse_duration_part(
    string: &str,
    delim: char,
) -> Result<(Decimal, &str), AerospikeConnectorError> {
    let idx = string.find(delim);
    let value = idx
        .map_or(Ok(Decimal::ZERO), |idx| string[..idx].parse())
        .map_err(|_| AerospikeConnectorError::ParsingDurationFailed)?;
    if let Some(idx) = idx {
        Ok((value, &string[idx + 1..]))
    } else {
        Ok((value, string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("PT3H").unwrap(), Duration::new(3600 * 3, 0));
        assert_eq!(parse_duration("PT3M").unwrap(), Duration::new(60 * 3, 0));
        assert_eq!(parse_duration("PT3S").unwrap(), Duration::new(3, 0));

        assert_eq!(
            parse_duration("PT3H3S").unwrap(),
            Duration::new(3600 * 3 + 3, 0)
        );

        assert_eq!(
            parse_duration("PT3.2H").unwrap(),
            Duration::new(3600 * 3 + 12 * 60, 0)
        );

        assert_eq!(
            parse_duration("PT3.2H").unwrap(),
            Duration::new(3600 * 3 + 12 * 60, 0)
        );
        assert!(parse_duration("PT3.2H2M").is_err());
        assert_eq!(
            parse_duration("PT0.000123S").unwrap(),
            Duration::new(0, 123_000)
        );
    }
}
