use std::collections::HashMap;

use crate::auth::Access;
use crate::grpc::common_grpc::common_grpc_service_server::CommonGrpcService;
use crate::grpc::internal_grpc::PipelineResponse;
use crate::grpc::shared_impl;
use crate::grpc::types_helper::{map_field_definitions, map_record};
use crate::{api_helper, RoCacheEndpoint};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::common_grpc::{
    CountResponse, GetEndpointsRequest, GetEndpointsResponse, GetFieldsRequest, GetFieldsResponse,
    OnEventRequest, QueryRequest, QueryResponse,
};
use crate::grpc::types::Operation;

type EventResult<T> = Result<Response<T>, Status>;
type ResponseStream = ReceiverStream<Result<Operation, tonic::Status>>;

// #[derive(Clone)]
pub struct CommonService {
    /// For look up endpoint from its name. `key == value.endpoint.name`.
    pub endpoint_map: HashMap<String, RoCacheEndpoint>,
    pub event_notifier: Option<tokio::sync::broadcast::Receiver<PipelineResponse>>,
}

impl CommonService {
    fn parse_request(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<(&RoCacheEndpoint, QueryRequest, Option<Access>), Status> {
        let parts = request.into_parts();
        let mut extensions = parts.1;
        let query_request = parts.2;
        let access = extensions.remove::<Access>();
        let endpoint = &query_request.endpoint;
        let cache_endpoint = self
            .endpoint_map
            .get(endpoint)
            .map_or(Err(Status::invalid_argument(endpoint)), Ok)?;
        Ok((cache_endpoint, query_request, access))
    }
}

#[tonic::async_trait]
impl CommonGrpcService for CommonService {
    async fn count(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<CountResponse>, Status> {
        let (pipeline_details, query_request, access) = self.parse_request(request)?;

        let count = shared_impl::count(pipeline_details, query_request.query.as_deref(), access)?;

        let reply = CountResponse {
            count: count as u64,
        };
        Ok(Response::new(reply))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let (pipeline_details, query_request, access) = self.parse_request(request)?;

        let (schema, records) =
            shared_impl::query(pipeline_details, query_request.query.as_deref(), access)?;

        let fields = map_field_definitions(schema.fields);
        let records = records.into_iter().map(map_record).collect();
        let reply = QueryResponse { fields, records };

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
            .endpoint_map
            .get(endpoint)
            .ok_or_else(|| Status::invalid_argument(endpoint))?;

        shared_impl::on_event(
            pipeline_details,
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
        let endpoints = self.endpoint_map.keys().cloned().collect();
        Ok(Response::new(GetEndpointsResponse { endpoints }))
    }

    async fn get_fields(
        &self,
        request: Request<GetFieldsRequest>,
    ) -> Result<Response<GetFieldsResponse>, Status> {
        let request = request.into_inner();
        let endpoint = request.endpoint;
        let pipeline_details = self
            .endpoint_map
            .get(&endpoint)
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let api_helper = api_helper::ApiHelper::new(pipeline_details, None)?;
        let schema = api_helper
            .get_schema()
            .map_or(Err(Status::invalid_argument(&endpoint)), Ok)?;

        let fields = map_field_definitions(schema.fields);

        let primary_index = schema.primary_index.iter().map(|f| *f as i32).collect();
        Ok(Response::new(GetFieldsResponse {
            primary_index,
            fields,
        }))
    }
}
