use super::util::{dozer_field_to_json_value, from_cache_error};
use crate::{api_helper, PipelineDetails};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::serde_json::{json, Map, Value};
use prost_reflect::DynamicMessage;
use tonic::{codegen::BoxFuture, Request, Response, Status};
pub struct ListService {
    pub(crate) pipeline_details: PipelineDetails,
}
impl ListService {}
impl tonic::server::UnaryService<DynamicMessage> for ListService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call<'a>(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let pipeline_details = self.pipeline_details.to_owned();
        let fut = async move { grpc_list(pipeline_details, request).await };
        Box::pin(fut)
    }
}

async fn grpc_list(
    pipeline_details: PipelineDetails,
    request: Request<DynamicMessage>,
) -> Result<Response<Value>, Status> {
    let _dynamic_message = request.into_inner();
    let exp = QueryExpression::new(None, vec![], 50, 0);
    let api_helper = api_helper::ApiHelper::new(pipeline_details, None)?;
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
