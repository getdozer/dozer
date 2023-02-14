use dozer_cache::cache::expression::{default_limit_for_query, QueryExpression};
use dozer_cache::cache::RecordWithId;
use dozer_cache::CacheReader;
use dozer_types::log::warn;
use dozer_types::serde_json;
use dozer_types::types::Schema;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Response, Status};

use crate::api_helper::ApiHelper;
use crate::auth::Access;

use super::internal_grpc::pipeline_response::ApiEvent;
use super::internal_grpc::PipelineResponse;
use super::types::Operation;

mod filter;

pub fn from_error(error: impl std::error::Error) -> Status {
    Status::new(Code::Internal, error.to_string())
}

fn parse_query(
    query: Option<&str>,
    default: impl FnOnce() -> QueryExpression,
) -> Result<QueryExpression, Status> {
    match query {
        Some(query) => {
            if query.is_empty() {
                Ok(default())
            } else {
                serde_json::from_str(query).map_err(from_error)
            }
        }
        None => Ok(default()),
    }
}

pub fn count(
    reader: &CacheReader,
    endpoint_name: &str,
    query: Option<&str>,
    access: Option<Access>,
) -> Result<usize, Status> {
    let query = parse_query(query, QueryExpression::with_no_limit)?;
    let api_helper = ApiHelper::new(reader, endpoint_name, access)?;
    api_helper.get_records_count(query).map_err(from_error)
}

pub fn query(
    reader: &CacheReader,
    endpoint_name: &str,
    query: Option<&str>,
    access: Option<Access>,
) -> Result<(Schema, Vec<RecordWithId>), Status> {
    let mut query = parse_query(query, QueryExpression::with_default_limit)?;
    if query.limit.is_none() {
        query.limit = Some(default_limit_for_query());
    }
    let api_helper = ApiHelper::new(reader, endpoint_name, access)?;
    let (schema, records) = api_helper.get_records(query).map_err(from_error)?;
    Ok((schema, records))
}

pub fn on_event<T: Send + 'static>(
    reader: &CacheReader,
    endpoint_name: &str,
    filter: Option<&str>,
    mut broadcast_receiver: Option<Receiver<PipelineResponse>>,
    access: Option<Access>,
    event_mapper: impl Fn(Operation, String) -> Option<T> + Send + Sync + 'static,
) -> Result<Response<ReceiverStream<T>>, Status> {
    if broadcast_receiver.is_none() {
        return Err(Status::unavailable(
            "on_event is not enabled. This is currently an experimental feature. Enable it in the config.",
        ));
    }

    let filter = match filter {
        Some(filter) => {
            if filter.is_empty() {
                None
            } else {
                Some(serde_json::from_str(filter).map_err(from_error)?)
            }
        }
        None => None,
    };
    let api_helper = ApiHelper::new(reader, endpoint_name, access)?;
    let schema = api_helper
        .get_schema()
        .map_err(|_| Status::invalid_argument(endpoint_name))?;

    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            if let Some(broadcast_receiver) = broadcast_receiver.as_mut() {
                let event = broadcast_receiver.recv().await;
                match event {
                    Ok(event) => {
                        if let Some(ApiEvent::Op(op)) = event.api_event {
                            if filter::op_satisfies_filter(&op, filter.as_ref(), &schema) {
                                if let Some(event) = event_mapper(op, event.endpoint) {
                                    if (tx.send(event).await).is_err() {
                                        // receiver dropped
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to receive event from broadcast channel: {}", e);
                        if e == RecvError::Closed {
                            break;
                        }
                    }
                }
            }
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}
