use super::{
    models::on_change::{EventType, OnChangeResponse},
    util::convert_grpc_message_to_query_exp,
};
use crate::{api_helper, api_server::PipelineDetails, errors::GRPCError};
use dozer_cache::cache::{expression::QueryExpression, Cache, LmdbCache};
use dozer_types::{
    errors::cache::CacheError,
    events::Event,
    serde_json::{self},
};
use prost_reflect::DynamicMessage;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
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
    let result = api_helper.get_record(id_input).map_err(from_cache_error)?;
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
        .get_records(exp)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

    let value_json = serde_json::to_value(result).map_err(GRPCError::SerizalizeError)?;
    // wrap to object
    let mut result_json: Map<String, Value> = Map::new();
    result_json.insert(pipeline_details.schema_name.to_lowercase(), value_json);
    let result = Value::Object(result_json);
    Ok(Response::new(result))
}

pub async fn grpc_server_stream(
    pipeline_details: PipelineDetails,
    cache: Arc<LmdbCache>,
    _: Request<DynamicMessage>,
    event_notifier: tokio::sync::broadcast::Receiver<Event>, //crossbeam::channel::Receiver<Event>,
) -> Result<Response<ReceiverStream<Result<Value, tonic::Status>>>, Status> {
    let api_helper = api_helper::ApiHelper::new(pipeline_details, cache, None)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    // create subscribe
    let mut broadcast_receiver = event_notifier.resubscribe();
    tokio::spawn(async move {
        while let Ok(event) = broadcast_receiver.recv().await {
            match event {
                Event::RecordInsert(record)
                | Event::RecordDelete(record)
                | Event::RecordUpdate(record) => {
                    let converted_record = api_helper.convert_record_to_json(record).unwrap();
                    let value_json = serde_json::to_value(converted_record)
                        .map_err(GRPCError::SerizalizeError)
                        .unwrap();
                    let my_detail: prost_wkt_types::Value =
                        serde_json::from_value(value_json).unwrap();
                    let event_response = super::models::on_change::Event {
                        r#type: EventType::RecordInsert as i32,
                        detail: Some(my_detail),
                    };
                    let on_change_response = OnChangeResponse {
                        event: Some(event_response),
                    };
                    let result_respone = serde_json::to_value(on_change_response).unwrap();
                    if (tx.send(Ok(result_respone)).await).is_err() {
                        // receiver drop
                        break;
                    }
                }
                _ => {}
            }
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}

fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}
