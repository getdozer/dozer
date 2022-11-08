use std::collections::HashMap;

use super::util::convert_grpc_message_to_query_exp;
use crate::{api_helper, api_server::PipelineDetails, errors::GRPCError};
use dozer_cache::cache::Cache;
use dozer_types::{
    field_to_json_value,
    rust_decimal::serde,
    serde_json::{self, Map, Value}, types::Field,
};
use heck::ToUpperCamelCase;
use prost_reflect::DynamicMessage;
use tonic::{codegen::BoxFuture, Code, Request, Response, Status};
pub struct QueryDynamicService {
    pub(crate) pipeline_details: HashMap<String, PipelineDetails>,
}
impl tonic::server::UnaryService<DynamicMessage> for QueryDynamicService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let pipeline_details = self.pipeline_details.to_owned();
        let fut = async move { grpc_query_dynamic(pipeline_details, request).await };
        Box::pin(fut)
    }
}
pub async fn grpc_query_dynamic(
    pipeline_details: HashMap<String, PipelineDetails>,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let dynamic_message = request.into_inner();
    let endpoint_param = dynamic_message
        .get_field_by_name("endpoint")
        .unwrap()
        .to_owned();
    let endpoint_str = endpoint_param.as_str().unwrap();
    println!("==== endpoint_param {:?}", endpoint_str);
    // get pipleline detail by endpoint
    let current_path_service_name = format!("Dozer.{}Service", endpoint_str.to_upper_camel_case());
    println!(
        "==== current_path_service_name {:?}",
        current_path_service_name
    );
    let pipeline_detail = pipeline_details[&current_path_service_name].to_owned();
    let api_helper = api_helper::ApiHelper::new(pipeline_detail.to_owned(), None)?;
    let exp = convert_grpc_message_to_query_exp(dynamic_message)?;
    let records = api_helper
        .get_records_raw(exp)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    // let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    let schema = pipeline_detail
        .cache_endpoint
        .cache
        .get_schema_by_name(&pipeline_detail.schema_name)
        .map_err(|e| Status::internal(e.to_string()))?;
    // println!("==== value_json {:?}", value_json);
    // let fields = result.iter().map(|k| k.keys()).collect();

    let record_values: Value = serde_json::to_value(
        records
            .iter()
            .map(|r| {
                r.values.to_owned()
                //  serde_json::to_value(r).unwrap()
            })
            .collect::<Vec<Vec<Field>>>(),
    )
    .unwrap();
    // let record_as_value = serde_json::to_value(records.iter().map(|r| r.values)).map_err(|e| Status::internal(e.to_string()))?;;

    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert(
        "fields".to_owned(),
        schema.fields.iter().map(|f| f.name.to_owned()).collect(),
    );
    result_json.insert("data".to_owned(), record_values);
    let result = Value::Object(result_json);
    println!("===== result {:?}", result);
    Ok(Response::new(result))
}
