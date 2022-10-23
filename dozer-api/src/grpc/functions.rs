use dozer_cache::cache::{expression::QueryExpression, LmdbCache};
use dozer_types::serde_json;
use prost_reflect::DynamicMessage;
use serde_json::{Map, Value};
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

use crate::api_helper::{get_record, get_records};

use super::util::convert_grpc_message_to_query_exp;
pub async fn grpc_list(
    schema_name: String,
    cache: Arc<LmdbCache>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let _dynamic_message = request.into_inner();
    let exp = QueryExpression::new(None, vec![], 50, 0);
    let result = get_records(&schema_name, cache, exp).unwrap();
    let value_json = serde_json::to_value(result).unwrap();
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert("films".to_owned(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}

pub async fn grpc_get_by_id(
    schema_name: String,
    cache: Arc<LmdbCache>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let dynamic_message = request.into_inner();
    // let options = SerializeOptions::new();
    // let mut serializer = serde_json::Serializer::new(vec![]);
    // dynamic_message
    //     .serialize_with_options(&mut serializer, &options)
    //     .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    // let string_utf8 = String::from_utf8(serializer.into_inner())
    //     .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    // let input_object: Value = serde_json::from_str(&string_utf8)
    //     .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    // let id_input = input_object.get("id").ok_or(Status::new(
    //     Code::Internal,
    //     "Cannot get input id".to_owned(),
    // ))?;

    let id_field = dynamic_message
        .get_field_by_name("id")
        .ok_or_else(|| Status::new(Code::Internal, "Cannot get input id".to_owned()))?;
    let id_input = id_field.as_str().unwrap_or_default();
    let result = get_record(&schema_name, cache, id_input.to_owned())
        .map_err(|err| Status::new(Code::NotFound, err.to_string()))?;
    let value_json =
        serde_json::to_value(result).map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(Response::new(value_json))
}

pub async fn grpc_query(
    schema_name: String,
    cache: Arc<LmdbCache>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let dynamic_message = request.into_inner();
    let exp = convert_grpc_message_to_query_exp(dynamic_message)?;
    let result = get_records(&schema_name, cache, exp)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

    let value_json =
        serde_json::to_value(result).map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert("films".to_owned(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}
