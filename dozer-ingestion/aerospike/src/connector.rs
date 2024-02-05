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
use actix_web::{get, HttpResponse};
use actix_web::post;
use actix_web::web;
use actix_web::App;
use actix_web::HttpRequest;
use actix_web::HttpServer;

use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};

#[derive(Debug, Error)]
pub enum AerospikeConnectorError {
    #[error("Cannot start server: {0}")]
    CannotStartServer(#[from] std::io::Error),

    #[error("No set name find in key: {0:?}")]
    NoSetNameFindInKey(Vec<Option<String>>),

    #[error("Set name is none. Key: {0:?}")]
    SetNameIsNone(Vec<Option<String>>),

    #[error("No PK in key: {0:?}")]
    NoPkInKey(Vec<Option<String>>),

    #[error("Invalid key value: {0:?}. Key is supposed to have 4 elements.")]
    InvalidKeyValue(Vec<Option<String>>),

    #[error("PK is none: {0:?}")]
    PkIsNone(Vec<Option<String>>),
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

    let operation_events = map_event(
        event,
        state.tables_index_map.clone(),
    )
        .await;

    match operation_event {
        Ok(None) => HttpResponse::Ok().finish(),
        Ok(Some(event)) => {
            state.ingestor
                .handle_message(event)
                .await
                .map_or_else(
                    |e| {
                        error!("Aerospike ingestion message send error: {:?}", e);
                        HttpResponse::InternalServerError().finish()
                    },
                    |_| HttpResponse::Ok().finish()
                )
        },
        Err(e) => map_error(e),
    }

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
        let schemas = table_infos
            .iter()
            .map(|s| {
                let primary_index = s.column_names.iter().position(|n| n == "PK").map_or(vec![], |i| vec![i]);
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
                        primary_index,
                    },
                    cdc_type: Default::default(),
                })
            })
            .collect();

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
            .await?;
        ingestor
            .handle_message(IngestionMessage::TransactionInfo(
                TransactionInfo::SnapshottingDone { id: None },
            ))
            .await?;

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

async fn map_event(
    event: AerospikeEvent,
    tables_map: HashMap<String, TableIndexMap>,
) -> Result<Option<Vec<IngestionMessage>>, AerospikeConnectorError> {
    let key = event.key.clone();
    if key.len() != 4 {
        return Err(AerospikeConnectorError::InvalidKeyValue(key.clone()));
    }

    let set_name = match key.clone().get(1) {
        None => Err(AerospikeConnectorError::NoSetNameFindInKey(key.clone())),
        Some(Some(set_name)) => Ok(set_name.clone()),
        Some(None) => Err(AerospikeConnectorError::SetNameIsNone(key.clone())),
    }?;

    if let Some(TableIndexMap {
        columns_map,
        table_index,
    }) = tables_map.get(set_name.as_str())
    {
        let mut fields = vec![Field::Null; columns_map.len()];
        if let Some(pk) = columns_map.get("PK") {
            fields[*pk] = key.clone().last()
                .map_or_else(
                    || Err(AerospikeConnectorError::NoPkInKey(key.clone())),
                    |value| {
                        value.clone().ok_or(
                            AerospikeConnectorError::PkIsNone(key.clone()),
                        )
                    }
                )
                .map(Field::String)?
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

        Ok(Some(vec![IngestionMessage::OperationEvent {
            table_index: *table_index,
            op: Insert {
                new: dozer_types::types::Record::new(fields),
            },
            id: None,
        }, IngestionMessage::TransactionInfo(TransactionInfo::Commit {
            id: None,
        })]))
    } else {
        Ok(None)
    }
}
