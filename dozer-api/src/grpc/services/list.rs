use super::util::from_cache_error;
use crate::errors::GRPCError;
use crate::{api_helper, api_server::PipelineDetails};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::serde_json::{self, Map, Value};
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
    let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), None)?;
    let result = api_helper.get_records(exp).map_err(from_cache_error)?;
    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert("data".to_owned(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}
