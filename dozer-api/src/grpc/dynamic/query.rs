use super::util::{convert_grpc_message_to_query_exp, dozer_field_to_json_value, from_cache_error};
use crate::{api_helper, PipelineDetails};
use dozer_types::serde_json::{json, Map, Value};
use prost_reflect::DynamicMessage;
use tonic::{codegen::BoxFuture, Request, Response, Status};
pub struct QueryService {
    pub(crate) pipeline_details: PipelineDetails,
}
impl tonic::server::UnaryService<DynamicMessage> for QueryService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let pipeline_details = self.pipeline_details.to_owned();
        let fut = async move { grpc_query(pipeline_details.to_owned(), request).await };
        Box::pin(fut)
    }
}
pub async fn grpc_query(
    pipeline_details: PipelineDetails,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let api_helper = api_helper::ApiHelper::new(pipeline_details, None)?;
    let dynamic_message = request.into_inner();
    let exp = convert_grpc_message_to_query_exp(dynamic_message)?;
    let (schema, records) = api_helper.get_records(exp).map_err(from_cache_error)?;

    let fields = schema.fields;
    let mut vec_json: Vec<Value> = vec![];
    for rec in records {
        let mut json_respone = json!({});
        let rect_value = rec.values.to_owned();
        for (idx, field) in fields.iter().enumerate() {
            json_respone[field.name.to_owned()] =
                dozer_field_to_json_value(&rect_value[idx]).unwrap();
        }
        vec_json.push(json_respone);
    }
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert("data".to_owned(), Value::Array(vec_json));
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}
