use std::collections::HashMap;
use std::ops::Index;
use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::json_types::JsonValue;
use dozer_ingestion_connector::dozer_types::log::info;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::IngestionMessage;
use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::dozer_types::types::Operation::Insert;
use dozer_ingestion_connector::dozer_types::types::{Field, FieldDefinition, FieldType, Schema};
use dozer_ingestion_connector::tokio::net::TcpListener;
use dozer_ingestion_connector::{
    async_trait, dozer_types, Connector, Ingestor, SourceSchema, SourceSchemaResult,
    TableIdentifier, TableInfo,
};
use dozer_ingestion_connector::dozer_types::arrow::ipc::Null;
use h2::server::handshake;
use http::StatusCode;

#[derive(Debug)]
pub struct AerospikeConnector {}

impl Default for AerospikeConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl AerospikeConnector {
    pub fn new() -> Self {
        Self {}
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
        Ok(vec![TableIdentifier {
            schema: None,
            name: "test".to_string(),
        }])
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
                        primary_index: vec![s.column_names.iter().position(|n| n == "PK").unwrap()]
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

        info!("Tables {:?}", tables);

        let mut tables_map = HashMap::new();
        tables.iter().for_each(|table| {
            let mut columns_map = HashMap::new();
            table.column_names.iter().enumerate().for_each(|(i, name)| {
                columns_map.insert(name.clone(), i);
            });
            tables_map.insert(table.name.clone(), columns_map);
        });

        info!("Tables map {:?}", tables_map);

        let listener = TcpListener::bind("127.0.0.1:5928").await.unwrap();

        loop {
            if let Ok((socket, _peer_addr)) = listener.accept().await {
                // Spawn a new task to process each connection.
                // Start the HTTP/2 connection handshake
                let mut h2 = handshake(socket).await.unwrap();
                // Accept all inbound HTTP/2 streams sent over the
                // connection.
                while let Some(request) = h2.accept().await {
                    let (request, mut respond) = request.unwrap();

                    let mut body = request.into_body();

                    // let mut byes = Vec::new();
                    while let Some(chunk) = body.data().await.transpose().unwrap() {
                        if !chunk.is_empty() {
                            info!("Got event");
                            let a = String::from_utf8(Vec::from(chunk));
                            let b = dozer_types::serde_json::from_str::<JsonValue>(&a.unwrap())
                                .unwrap();

                            let bins = b.get("bins").unwrap().as_array().unwrap();
                            let key_array = b.get("key").unwrap().as_array().unwrap();
                            let table_name = key_array.first().unwrap().as_string().unwrap();
                            if let Some(columns_map) = tables_map.get(table_name.as_str()) {

                            let mut fields = vec![Field::Null; columns_map.len()];
                            if let Some(pk) = columns_map.get("PK") {
                                fields[*pk] = Field::String(key_array.last().unwrap().as_string().unwrap().to_string());
                            }

                            for bin in bins {
                                let bin = bin.as_object().unwrap();
                                let name = bin.get("name").unwrap().as_string().unwrap();
                                if let Some(i) = columns_map.get(name.as_str()) {
                                    let typ = bin.get("type").unwrap().as_string().unwrap();
                                    if typ == "str" {
                                        let value = bin.get("value").unwrap().as_string().unwrap();
                                        fields[*i] = Field::String(value.to_string());
                                    }
                                    if typ == "int" {
                                        let value = bin.get("value").unwrap().to_i64().unwrap();
                                        fields[*i] = Field::String(value.to_string());
                                    }
                                }
                            }

                            info!("Fields {:?}", fields);

                            ingestor
                                .handle_message(IngestionMessage::OperationEvent {
                                    table_index: 0,
                                    op: Insert {
                                        new: dozer_types::types::Record::new(fields),
                                    },
                                    id: None,
                                })
                                .await
                                .unwrap();
                                }
                        }
                    }

                    info!("Got HTTP/2 request");
                    // Build a response with no body
                    let response = http::response::Response::builder()
                        .status(StatusCode::OK)
                        .body(())
                        .unwrap();

                    // Send the response back to the client
                    respond.send_response(response, true).unwrap();
                }
            }
        }

        Ok(())
    }
}
