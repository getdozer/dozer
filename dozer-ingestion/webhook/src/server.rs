use crate::{
    util::{extract_source_schema, map_record},
    Error,
};
use actix_web::{
    web::{self, Data},
    App, HttpRequest, HttpServer, Responder,
};
use dozer_ingestion_connector::{
    dozer_types::{
        models::ingestion_types::{IngestionMessage, WebhookConfig, WebhookVerb},
        serde_json,
        types::{Operation, Record},
    },
    Ingestor, SourceSchema, TableInfo,
};
use std::{collections::HashMap, sync::Arc};

pub(crate) struct WebhookServer {
    config: WebhookConfig,
}

impl WebhookServer {
    pub(crate) fn new(config: WebhookConfig) -> Self {
        Self { config }
    }

    pub(crate) async fn start(
        &self,
        ingestor: Arc<Ingestor>,
        tables: Vec<TableInfo>,
    ) -> Result<(), Error> {
        let config = self.config.clone();
        // Clone or extract necessary data from `self`
        let server = HttpServer::new(move || {
            let mut app = App::new();

            for endpoint in config.endpoints.iter() {
                let endpoint_data = endpoint.clone();
                let source_schema_dict = extract_source_schema(endpoint_data.to_owned().schema);
                let tables = tables.clone();
                let mut app_resource = web::resource(endpoint_data.path)
                    .app_data(web::Data::new(Arc::clone(&ingestor)))
                    .app_data(web::Data::new(source_schema_dict))
                    .app_data(web::Data::new(tables));
                for verb in &endpoint.verbs {
                    app_resource = match verb {
                        WebhookVerb::POST => app_resource.route(web::post().to(Self::post_handler)),
                        WebhookVerb::DELETE => {
                            app_resource.route(web::delete().to(Self::delete_handler))
                        }
                        _ => app_resource.route(web::route().to(Self::other_handler)),
                    };
                }
                app = app.service(app_resource);
            }
            app
        });

        let host = config
            .host
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = config.port.unwrap_or(8080);
        let address = format!("{}:{}", host, port); // Format host and port into a single string
        let server = server.bind(address)?.run();
        server.await.map_err(Into::into)
    }

    fn common_handler(
        tables: Data<Vec<TableInfo>>,
        schema_dict: Data<HashMap<String, SourceSchema>>,
        info: web::Json<serde_json::Value>,
    ) -> actix_web::Result<Vec<(usize, Vec<Record>)>, actix_web::error::Error> {
        let source_schema_dict = schema_dict.get_ref();
        let info = &info.into_inner();

        let mut result: Vec<(usize, Vec<Record>)> = vec![];
        if let serde_json::Value::Object(object) = info {
            for (schema_name, values) in object.iter() {
                let schema = match source_schema_dict.get(schema_name) {
                    Some(schema) => schema,
                    None => return Err(actix_web::error::ErrorBadRequest("Invalid schema name")),
                };
                let records = match values.as_array() {
                    Some(values_arr) => values_arr
                        .iter()
                        .map(|value_element| {
                            let value = value_element.as_object().ok_or_else(|| {
                                actix_web::error::ErrorBadRequest("Invalid value")
                            })?;
                            map_record(value.to_owned(), &schema.schema)
                                .map_err(actix_web::error::ErrorBadRequest)
                        })
                        .collect::<Result<Vec<Record>, _>>(),
                    None => {
                        let value = values
                            .as_object()
                            .ok_or_else(|| actix_web::error::ErrorBadRequest("Invalid value"))?;
                        map_record(value.to_owned(), &schema.schema)
                            .map(|e| vec![e])
                            .map_err(|e| actix_web::error::ErrorBadRequest(e.to_string()))
                    }
                }?;
                let table_idx = tables
                    .iter()
                    .position(|table| table.name.as_str() == schema_name)
                    .ok_or_else(|| actix_web::error::ErrorBadRequest("Invalid table name"))?;
                result.push((table_idx, records));
            }
        } else {
            return Err(actix_web::error::ErrorBadRequest("Invalid JSON"));
        }
        Ok(result)
    }

    async fn post_handler(
        ingestor: Data<Arc<Ingestor>>,
        schema_dict: Data<HashMap<String, SourceSchema>>,
        tables: Data<Vec<TableInfo>>,
        info: web::Json<serde_json::Value>,
    ) -> actix_web::Result<impl Responder> {
        let ingestor = ingestor.get_ref();
        let records = Self::common_handler(tables, schema_dict, info)?;

        for (table_idx, records) in records {
            let op: IngestionMessage = if records.len() == 1 {
                IngestionMessage::OperationEvent {
                    table_index: table_idx,
                    op: Operation::Insert {
                        new: records[0].clone(),
                    },
                    state: None,
                }
            } else {
                IngestionMessage::OperationEvent {
                    table_index: table_idx,
                    op: Operation::BatchInsert { new: records },
                    state: None,
                }
            };
            ingestor
                .handle_message(op)
                .await
                .map_err(|e| actix_web::error::ErrorInternalServerError(format!("Error: {}", e)))?;
        }

        let json_response = serde_json::json!({
            "status": "ok"
        });

        Ok(web::Json(json_response))
    }

    async fn delete_handler(
        ingestor: Data<Arc<Ingestor>>,
        schema_dict: Data<HashMap<String, SourceSchema>>,
        tables: Data<Vec<TableInfo>>,
        info: web::Json<serde_json::Value>,
    ) -> actix_web::Result<impl Responder> {
        let ingestor = ingestor.get_ref();
        let records = Self::common_handler(tables, schema_dict, info)?;
        for (table_idx, records) in records {
            for record in records {
                let op: IngestionMessage = IngestionMessage::OperationEvent {
                    table_index: table_idx,
                    op: Operation::Delete { old: record },
                    state: None,
                };
                ingestor.handle_message(op).await.map_err(|e| {
                    actix_web::error::ErrorInternalServerError(format!("Error: {}", e))
                })?;
            }
        }

        let json_response = serde_json::json!({
            "status": "ok"
        });

        Ok(web::Json(json_response))
    }
    async fn other_handler(req: HttpRequest) -> actix_web::Result<impl Responder> {
        // get VERB from request
        let verb = req.method().as_str();
        let json_response = serde_json::json!({
            "status": format!("{} not supported", verb)
        });
        Ok(web::Json(json_response))
    }
}
