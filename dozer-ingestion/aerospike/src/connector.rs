use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::log::{error, info};
use dozer_ingestion_connector::dozer_types::models::connection::AerospikeConnection;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    IngestionMessage, TransactionInfo,
};
use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::dozer_types::types::Operation::Insert;
use dozer_ingestion_connector::dozer_types::types::{Field, FieldDefinition, FieldType, Schema};
use dozer_ingestion_connector::{
    async_trait, dozer_types, Connector, Ingestor, SourceSchema, SourceSchemaResult,
    TableIdentifier, TableInfo,
};
use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::serde::Deserialize;

use actix_web::dev::Server;
use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::App;
use actix_web::HttpRequest;
use actix_web::HttpServer;
use actix_web::Responder;
use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};

#[derive(Debug, Error)]
pub enum AerospikeConnectorError {
    #[error("Cannot start server: {0}")]
    CannotStartServer(#[from] std::io::Error),
}
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

#[get("/")]
async fn healthcheck(_req: HttpRequest) -> impl Responder {
    "OK"
}

#[post("/")]
async fn event_request_handler(
    json: web::Json<AerospikeEvent>,
    data: web::Data<ServerState>,
) -> impl Responder {
    let event = json.into_inner();
    let state = data.into_inner();

    process_event(
        event,
        state.tables_index_map.clone(),
        state.ingestor.clone(),
    )
    .await;

    "OK"
}

#[derive(Clone)]
struct TableIndexMap {
    table_index: usize,
    columns_map: HashMap<String, usize>,
}

#[derive(Clone)]
struct ServerState {
    tables_index_map: HashMap<String, TableIndexMap>,
    ingestor: Ingestor,
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
            .handle_message(IngestionMessage::TransactionInfo(
                TransactionInfo::SnapshottingStarted,
            ))
            .await
            .unwrap();
        ingestor
            .handle_message(IngestionMessage::TransactionInfo(
                TransactionInfo::SnapshottingDone { id: None },
            ))
            .await
            .unwrap();

        let tables_index_map: HashMap<String, TableIndexMap> = tables
            .iter()
            .enumerate()
            .map(|(table_index, table)| {
                let columns_map: HashMap<String, usize> = table
                    .column_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| (name.clone(), i))
                    .collect();

                (
                    table.name.clone(),
                    TableIndexMap {
                        table_index,
                        columns_map,
                    },
                )
            })
            .collect();

        let server_state = ServerState {
            tables_index_map: tables_index_map.clone(),
            ingestor: ingestor.clone(),
        };

        let _server = self.start_server(server_state)?.await;

        Ok(())
    }
}

async fn process_event(
    event: AerospikeEvent,
    tables_map: HashMap<String, TableIndexMap>,
    ingestor: Ingestor,
) {
    let table_name = event.key.get(1).unwrap().clone().unwrap();
    if let Some(TableIndexMap {
        columns_map,
        table_index,
    }) = tables_map.get(table_name.as_str())
    {
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

        ingestor
            .handle_message(IngestionMessage::OperationEvent {
                table_index: *table_index,
                op: Insert {
                    new: dozer_types::types::Record::new(fields),
                },
                id: None,
            })
            .await;
        let _ = ingestor
            .handle_message(IngestionMessage::TransactionInfo(TransactionInfo::Commit {
                id: None,
            }))
            .await;
    } else {
        // info!("Not found table: {}", table_name);
    }
}
