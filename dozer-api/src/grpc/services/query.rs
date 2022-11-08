use super::util::convert_grpc_message_to_query_exp;
use crate::{api_helper, api_server::PipelineDetails, errors::GRPCError};
use dozer_types::serde_json::{self, Map, Value};
use prost_reflect::DynamicMessage;
use tonic::{codegen::BoxFuture, Code, Request, Response, Status};
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
    let api_helper = api_helper::ApiHelper::new(pipeline_details.to_owned(), None)?;
    let dynamic_message = request.into_inner();
    let exp = convert_grpc_message_to_query_exp(dynamic_message)?;
    let result = api_helper
        .get_records(exp)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert("data".to_owned(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}
