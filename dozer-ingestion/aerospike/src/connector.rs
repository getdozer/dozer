use dozer_ingestion_connector::dozer_types::bytes;
use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::log::{error, info};
use dozer_ingestion_connector::dozer_types::models::connection::AerospikeConnection;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::IngestionMessage;
use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::dozer_types::types::Operation::Insert;
use dozer_ingestion_connector::dozer_types::types::{Field, FieldDefinition, FieldType, Schema};
use dozer_ingestion_connector::tokio::net::TcpListener;
use dozer_ingestion_connector::{
    async_trait, dozer_types, Connector, Ingestor, SourceSchema, SourceSchemaResult,
    TableIdentifier, TableInfo,
};
use h2::server::{handshake, SendResponse};
use http::StatusCode;
use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::serde::Deserialize;
use dozer_ingestion_connector::tokio;

#[derive(Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
pub struct AerospikeEvent {
    // msg: String,
    key: Vec<Option<String>>,
    // gen: u32,
    // exp: u32,
    // lut: u64,
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
}

impl AerospikeConnector {
    pub fn new(config: AerospikeConnection) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for AerospikeConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
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
        info!("Table infos: {:?}", table_infos);
        let schemas = table_infos
            .iter()
            .map(|s| {
                Ok(SourceSchema {
                    schema: Schema {
                        fields: s
                            .column_names
                            .iter()
                            .map(|name| FieldDefinition {
                                name: name.clone(),
                                typ: FieldType::String,
                                nullable: true,
                                source: Default::default(),
                            })
                            .collect(),
                        primary_index: vec![s.column_names.iter().position(|n| n == "PK").unwrap()],
                    },
                    cdc_type: Default::default(),
                })
            })
            .collect();

        info!("Schemas {:?}", schemas);
        Ok(schemas)
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
        ingestor
            .handle_message(IngestionMessage::SnapshottingStarted)
            .await
            .unwrap();
        ingestor
            .handle_message(IngestionMessage::SnapshottingDone { id: None })
            .await
            .unwrap();

        let mut tables_map = HashMap::new();
        let mut tables_idx = HashMap::new();
        tables.iter().enumerate().for_each(|(idx, table)| {
            let mut columns_map = HashMap::new();
            table.column_names.iter().enumerate().for_each(|(i, name)| {
                columns_map.insert(name.clone(), i);
            });
            tables_map.insert(table.name.clone(), columns_map);
            tables_idx.insert(table.name.clone(), idx);
        });

        let listener = TcpListener::bind("127.0.0.1:5928").await.unwrap();

        let is_batch = self.config.batching;
        loop {
            if let Ok((socket, _peer_addr)) = listener.accept().await {
                let t: HashMap<String, HashMap<String, usize>> = tables_map.clone();
                let i = ingestor.clone();
                let indexes = tables_idx.clone();

                tokio::spawn(async move {
                    let mut h2 = handshake(socket).await.expect("Connection");
                    // Accept all inbound HTTP/2 streams sent over the
                    // connection.

                    'looop: while let Some(request) = h2.accept().await {
                        match request {
                            Ok((request, mut respond)) => {
                                let mut body = request.into_body();

                                let mut parts = Vec::new();
                                while let Some(chunk) = body.data().await.transpose().unwrap() {
                                    let len = chunk.len();
                                    if !chunk.is_empty() {
                                        let event_string = String::from_utf8(Vec::from(chunk));
                                        if let Ok(event_string) = event_string {
                                            parts.push(event_string);
                                        }
                                    }

                                    let _ = body.flow_control().release_capacity(len);
                                }

                                if !parts.is_empty() {
                                    let event_string = parts.join("");

                                    let events = match is_batch {
                                        true => {
                                            let events = match dozer_types::serde_json::from_str::<
                                                Vec<AerospikeEvent>,
                                            >(
                                                &event_string.clone()
                                            ) {
                                                Ok(events) => events,
                                                Err(e) => {
                                                    error!("Error : {:?} {:?}", e, event_string);
                                                    send_bad_response(respond);
                                                    continue;
                                                }
                                            };
                                            events
                                        }
                                        false => {
                                            let event = match dozer_types::serde_json::from_str::<
                                                AerospikeEvent,
                                            >(
                                                &event_string.clone()
                                            ) {
                                                Ok(event) => event,
                                                Err(e) => {
                                                    error!("Error : {:?} {:?}", e, event_string);
                                                    send_bad_response(respond);
                                                    continue;
                                                }
                                            };
                                            vec![event]
                                        }
                                    };
                                    for evt in events {
                                        process_event(evt, t.clone(), indexes.clone(), i.clone())
                                            .await
                                    }
                                }

                                // info!("Got HTTP/2 request");
                                // Build a response with no body
                                let response = http::response::Response::builder()
                                    .status(StatusCode::OK)
                                    .body(())
                                    .unwrap();

                                // Send the response back to the client
                                respond.send_response(response, true).unwrap();
                            }
                            Err(e) => {
                                error!("Listiner Error: {:?}", e);
                                break 'looop;
                            }
                        }
                    }
                });
            }
        }
    }
}

fn send_bad_response(mut respond: SendResponse<bytes::Bytes>) {
    let response = http::response::Response::builder()
        .status(StatusCode::OK)
        .body(())
        .unwrap();

    // Send the response back to the client
    respond.send_response(response, true).unwrap();
}
async fn process_event(
    event: AerospikeEvent,
    tables_map: HashMap<String, HashMap<String, usize>>,
    tables_idx: HashMap<String, usize>,
    ingestor: Ingestor,
) {
    let table_name = event.key.get(1).unwrap().clone().unwrap();
    if let Some(columns_map) = tables_map.get(table_name.as_str()) {
        let mut fields = vec![Field::Null; columns_map.len()];
        if let Some(pk) = columns_map.get("PK") {
            fields[*pk] = match event.key.last().unwrap() {
                None => Field::Null,
                Some(s) => Field::String(s.to_string()),
            };
        }

        for bin in event.bins {
            if let Some(i) = columns_map.get(bin.name.as_str()) {
                match bin.value {
                    Some(value) => match bin.r#type.as_str() {
                        "str" => {
                            if let dozer_types::serde_json::Value::String(s) = value {
                                fields[*i] = Field::String(s);
                            }
                        }
                        "int" => {
                            if let dozer_types::serde_json::Value::Number(n) = value {
                                fields[*i] = Field::String(n.to_string());
                            }
                        }
                        "float" => {
                            if let dozer_types::serde_json::Value::Number(n) = value {
                                fields[*i] = Field::String(n.to_string());
                            }
                        }
                        unexpected => {
                            error!("Unexpected type: {}", unexpected);
                        }
                    },
                    None => {
                        fields[*i] = Field::Null;
                    }
                }
            }
        }

        let table_index = tables_idx.get(table_name.as_str()).unwrap().to_owned();
        ingestor
            .handle_message(IngestionMessage::OperationEvent {
                table_index,
                op: Insert {
                    new: dozer_types::types::Record::new(fields),
                },
                id: None,
            })
            .await
            .unwrap();
    } else {
        // info!("Not found table: {}", table_name);
    }
}
