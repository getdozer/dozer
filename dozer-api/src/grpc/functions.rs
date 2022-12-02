use super::util::convert_grpc_message_to_query_exp;
use crate::{api_helper, api_server::PipelineDetails, errors::GRPCError};
use dozer_cache::cache::{expression::QueryExpression, Cache, LmdbCache};
use dozer_cache::errors::CacheError;
use dozer_types::serde_json;
use prost_reflect::DynamicMessage;
use serde_json::{Map, Value};
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

pub async fn grpc_list(
    pipeline_details: PipelineDetails,
    cache: Arc<LmdbCache>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let _dynamic_message = request.into_inner();
    let exp = QueryExpression::new(None, vec![], 50, 0);
    let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), cache, None)?;
    let result = api_helper.get_records(exp).map_err(from_cache_error)?;
    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert(pipeline_details.schema_name.to_lowercase(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}

pub async fn grpc_get_by_id(
    pipeline_details: PipelineDetails,
    cache: Arc<LmdbCache>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let api_helper =
        api_helper::ApiHelper::new(pipeline_details.to_owned(), cache.to_owned(), None)?;
    let dynamic_message = request.into_inner();
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
    let result = api_helper
        .get_record_map(id_input)
        .map_err(from_cache_error)?;
    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert(pipeline_details.schema_name.to_lowercase(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}

pub async fn grpc_query(
    pipeline_details: PipelineDetails,
    cache: Arc<LmdbCache>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), cache, None)?;
    let dynamic_message = request.into_inner();
    let exp = convert_grpc_message_to_query_exp(dynamic_message)?;
    let result = api_helper
        .get_records_map(exp)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert(pipeline_details.schema_name.to_lowercase(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}

fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}
