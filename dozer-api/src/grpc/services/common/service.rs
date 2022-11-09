use std::collections::HashMap;

use crate::grpc::services::common::common_grpc::common_grpc_service_server::CommonGrpcService;
use crate::grpc::services::common::common_grpc::{Record, Value};
use crate::{api_helper, api_server::PipelineDetails};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::events::Event;
use dozer_types::log::warn;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::common_grpc::{FieldDefinition, OnEventRequest, QueryRequest, QueryResponse};
use super::helper;

type EchoResult<T> = Result<Response<T>, Status>;
type ResponseStream = ReceiverStream<Result<Record, tonic::Status>>;

// #[derive(Clone)]
pub struct ApiService {
    pub pipeline_map: HashMap<String, PipelineDetails>,
    pub event_notifier: tokio::sync::broadcast::Receiver<Event>,
}
#[tonic::async_trait]
impl CommonGrpcService for ApiService {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let request = request.into_inner();
        let endpoint = request.endpoint;
        let pipeline_details = self
            .pipeline_map
            .get(&endpoint)
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), None)?;

        let query: QueryExpression =
            dozer_types::serde_json::from_str(&request.query.map_or("{}".to_string(), |f| f))
                .map_err(|e| Status::from_error(Box::new(e)))?;

        let (schema, records) = api_helper
            .get_records(query)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        let fields = schema
            .fields
            .iter()
            .map(|f| FieldDefinition {
                typ: helper::map_field_type_to_pb(&f.typ) as i32,
                name: f.name.to_owned(),
                nullable: f.nullable,
            })
            .collect();
        let records: Vec<Record> = records
            .iter()
            .map(|r| {
                let values: Vec<Value> = r
                    .to_owned()
                    .values
                    .iter()
                    .map(helper::field_to_prost_value)
                    .collect();

                Record { values }
            })
            .collect();
        let reply = QueryResponse { fields, records };

        Ok(Response::new(reply))
    }
    #[allow(non_camel_case_types)]
    type onEventStream = ResponseStream;
    async fn on_event(&self, request: Request<OnEventRequest>) -> EchoResult<Self::onEventStream> {
        let request = request.into_inner();

        for endpoint in request.endpoints {
            let _pipeline_details = self
                .pipeline_map
                .get(&endpoint)
                .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        // create subscribe
        let mut broadcast_receiver = self.event_notifier.resubscribe();

        tokio::spawn(async move {
            loop {
                let receiver_event = broadcast_receiver.recv().await;
                match receiver_event {
                    Ok(event) => {
                        if let Event::RecordInsert(record) = event {
                            let values: Vec<Value> = record
                                .to_owned()
                                .values
                                .iter()
                                .map(helper::field_to_prost_value)
                                .collect();

                            let record = Record { values };

                            if (tx.send(Ok(record)).await).is_err() {
                                warn!("on_insert_grpc_server_stream receiver drop");
                                // receiver drop
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        warn!("on_insert_grpc_server_stream receiv Error: {:?}", error);
                        match error {
                            tokio::sync::broadcast::error::RecvError::Closed => {
                                break;
                            }
                            tokio::sync::broadcast::error::RecvError::Lagged(_) => {}
                        }
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
