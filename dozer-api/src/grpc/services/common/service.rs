use std::collections::HashMap;

use crate::grpc::services::common::common_grpc::common_grpc_service_server::CommonGrpcService;
use crate::grpc::services::common::common_grpc::{Record, Value};
use crate::{api_helper, api_server::PipelineDetails};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::events::ApiEvent;
use dozer_types::log::warn;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::common_grpc::{
    FieldDefinition, GetEndpointsRequest, GetEndpointsResponse, GetFieldsRequest,
    GetFieldsResponse, OnEventRequest, Operation, QueryRequest, QueryResponse,
};
use super::helper;

type EventResult<T> = Result<Response<T>, Status>;
type ResponseStream = ReceiverStream<Result<Operation, tonic::Status>>;

// #[derive(Clone)]
pub struct ApiService {
    pub pipeline_map: HashMap<String, PipelineDetails>,
    pub event_notifier: tokio::sync::broadcast::Receiver<ApiEvent>,
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

    async fn get_endpoints(
        &self,
        _: Request<GetEndpointsRequest>,
    ) -> Result<Response<GetEndpointsResponse>, Status> {
        let endpoints = self.pipeline_map.iter().map(|(k, _)| k).cloned().collect();
        Ok(Response::new(GetEndpointsResponse { endpoints }))
    }

    async fn get_fields(
        &self,
        request: Request<GetFieldsRequest>,
    ) -> Result<Response<GetFieldsResponse>, Status> {
        let request = request.into_inner();
        let endpoint = request.endpoint;
        let pipeline_details = self
            .pipeline_map
            .get(&endpoint)
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), None)?;
        let schema = api_helper
            .get_schema()
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let fields: Vec<FieldDefinition> = schema
            .fields
            .iter()
            .map(|f| FieldDefinition {
                typ: helper::map_field_type_to_pb(&f.typ) as i32,
                name: f.name.to_owned(),
                nullable: f.nullable,
            })
            .collect();

        let primary_index = schema.primary_index.iter().map(|f| *f as i32).collect();
        Ok(Response::new(GetFieldsResponse {
            primary_index,
            fields,
        }))
    }

    #[allow(non_camel_case_types)]
    type onEventStream = ResponseStream;
    async fn on_event(&self, request: Request<OnEventRequest>) -> EventResult<Self::onEventStream> {
        let request = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        // create subscribe
        let mut broadcast_receiver = self.event_notifier.resubscribe();

        tokio::spawn(async move {
            loop {
                let receiver_event = broadcast_receiver.recv().await;
                match receiver_event {
                    Ok(event) => {
                        if let ApiEvent::Operation(endpoint_name, operation) = event {
                            if endpoint_name == request.endpoint {
                                let op = helper::map_operation(endpoint_name, &operation);
                                if (tx.send(Ok(op)).await).is_err() {
                                    warn!("on_insert_grpc_server_stream receiver drop");
                                    // receiver drop
                                    break;
                                }
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
