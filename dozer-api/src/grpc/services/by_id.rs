use super::util::from_cache_error;
use crate::{api_helper, api_server::PipelineDetails, errors::GRPCError};
use dozer_cache::cache::Cache;
use dozer_types::serde_json::{self, Map, Value};
use prost_reflect::DynamicMessage;
use tonic::{codegen::BoxFuture, Code, Request, Response, Status};

pub struct GetByIdService {
    pub(crate) pipeline_details: PipelineDetails,
}
impl tonic::server::UnaryService<DynamicMessage> for GetByIdService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let pipeline_details = self.pipeline_details.to_owned();
        let fut = async move { grpc_get_by_id(pipeline_details.to_owned(), request).await };
        Box::pin(fut)
    }
}

async fn grpc_get_by_id(
    pipeline_details: PipelineDetails,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), None)?;
    let dynamic_message = request.into_inner();
    let cache = pipeline_details.cache_endpoint.cache.to_owned();
    let schema = cache
        .get_schema_by_name(&pipeline_details.schema_name)
        .map_err(from_cache_error)?;
    let primary_idx = schema.primary_index.first().ok_or_else(|| {
        GRPCError::MissingPrimaryKeyToQueryById(pipeline_details.schema_name.to_owned())
    })?;
    let primary_field = schema.fields[primary_idx.to_owned()].to_owned();

    let id_field = dynamic_message
        .get_field_by_name(&primary_field.name)
        .ok_or_else(|| Status::new(Code::Internal, "Cannot get input id".to_owned()))?;
    let id_input = id_field.to_string();
    let result = api_helper.get_record(id_input).map_err(from_cache_error)?;
    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert(pipeline_details.schema_name.to_lowercase(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}
