use std::collections::HashMap;

use crate::auth::Access;
use crate::grpc::common_grpc::common_grpc_service_server::CommonGrpcService;
use crate::grpc::internal_grpc::PipelineResponse;
use crate::grpc::shared_impl;
use crate::grpc::types_helper::map_record;
use crate::{api_helper, PipelineDetails};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::super::types_helper;
use crate::grpc::common_grpc::{
    GetEndpointsRequest, GetEndpointsResponse, GetFieldsRequest, GetFieldsResponse, HealthRequest,
    HealthResponse, OnEventRequest, QueryRequest, QueryResponse,
};
use crate::grpc::types::{FieldDefinition, Operation, Record};

type EventResult<T> = Result<Response<T>, Status>;
type ResponseStream = ReceiverStream<Result<Operation, tonic::Status>>;

// #[derive(Clone)]
pub struct CommonService {
    pub pipeline_map: HashMap<String, PipelineDetails>,
    pub event_notifier: Option<tokio::sync::broadcast::Receiver<PipelineResponse>>,
}

#[tonic::async_trait]
impl CommonGrpcService for CommonService {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let parts = request.into_parts();
        let extensions = parts.1;
        let query_request = parts.2;
        let access = extensions.get::<Access>();
        let endpoint = query_request.endpoint;
        let pipeline_details = self
            .pipeline_map
            .get(&endpoint)
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let (schema, records) = shared_impl::query(
            pipeline_details.clone(),
            query_request.query.as_deref(),
            access,
        )?;

        let fields = schema
            .fields
            .into_iter()
            .map(|f| FieldDefinition {
                typ: types_helper::map_field_type_to_pb(f.typ) as i32,
                name: f.name,
                nullable: f.nullable,
            })
            .collect();
        let records: Vec<Record> = records.into_iter().map(map_record).collect();
        let reply = QueryResponse { fields, records };

        Ok(Response::new(reply))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let status = "success".to_string();
        let reply = HealthResponse { status };
        Ok(Response::new(reply))
    }

    type OnEventStream = ResponseStream;

    async fn on_event(&self, request: Request<OnEventRequest>) -> EventResult<Self::OnEventStream> {
        let parts = request.into_parts();
        let extensions = parts.1;
        let query_request = parts.2;
        let access = extensions.get::<Access>();
        let endpoint = &query_request.endpoint;
        let pipeline_details = self
            .pipeline_map
            .get(endpoint)
            .ok_or_else(|| Status::invalid_argument(endpoint))?;

        shared_impl::on_event(
            pipeline_details.clone(),
            query_request.filter.as_deref(),
            self.event_notifier.as_ref().map(|r| r.resubscribe()),
            access.cloned(),
            move |op, endpoint| {
                if endpoint == query_request.endpoint {
                    Some(Ok(op))
                } else {
                    None
                }
            },
        )
    }

    async fn get_endpoints(
        &self,
        _: Request<GetEndpointsRequest>,
    ) -> Result<Response<GetEndpointsResponse>, Status> {
        let endpoints = self.pipeline_map.keys().cloned().collect();
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

        let api_helper = api_helper::ApiHelper::new(pipeline_details.clone(), None)?;
        let schema = api_helper
            .get_schema()
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let fields: Vec<FieldDefinition> = schema
            .fields
            .iter()
            .map(|f| FieldDefinition {
                typ: types_helper::map_field_type_to_pb(f.typ) as i32,
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
}
