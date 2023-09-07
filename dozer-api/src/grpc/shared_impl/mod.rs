use std::collections::HashMap;

use dozer_cache::cache::expression::{default_limit_for_query, FilterExpression, QueryExpression};
use dozer_cache::cache::CacheRecord;
use dozer_cache::CacheReader;
use dozer_types::grpc_types::types::Operation;
use dozer_types::log::warn;
use dozer_types::serde_json;
use dozer_types::types::Schema;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Response, Status};

use crate::api_helper::{get_records, get_records_count};
use crate::auth::Access;

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
    query: Option<&str>,
    endpoint: &str,
    access: Option<Access>,
) -> Result<usize, Status> {
    let mut query = parse_query(query, QueryExpression::with_no_limit)?;
    Ok(get_records_count(reader, &mut query, endpoint, access)?)
}

pub fn query(
    reader: &CacheReader,
    query: Option<&str>,
    endpoint: &str,
    access: Option<Access>,
) -> Result<Vec<CacheRecord>, Status> {
    let mut query = parse_query(query, QueryExpression::with_default_limit)?;
    if query.limit.is_none() {
        query.limit = Some(default_limit_for_query());
    }
    let records = get_records(reader, &mut query, endpoint, access)?;
    Ok(records)
}

#[derive(Debug)]
pub struct EndpointFilter {
    schema: Schema,
    filter: Option<FilterExpression>,
}

impl EndpointFilter {
    pub fn new(schema: Schema, filter: Option<&str>) -> Result<Self, Status> {
        let filter = filter
            .and_then(|filter| {
                if filter.is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(filter))
                }
            })
            .transpose()
            .map_err(from_error)?;
        Ok(Self { schema, filter })
    }
}

pub fn on_event<T: Send + 'static>(
    endpoints: HashMap<String, EndpointFilter>,
    mut broadcast_receiver: Option<Receiver<Operation>>,
    _access: Option<Access>,
    event_mapper: impl Fn(Operation) -> T + Send + Sync + 'static,
) -> Result<Response<ReceiverStream<T>>, Status> {
    // TODO: Use access.

    if broadcast_receiver.is_none() {
        return Err(Status::unavailable(
            "on_event is not enabled. This is currently an experimental feature. Enable it in the config.",
        ));
    }

    if endpoints.is_empty() {
        return Err(Status::invalid_argument("empty endpoints array"));
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            if let Some(broadcast_receiver) = broadcast_receiver.as_mut() {
                let event = broadcast_receiver.recv().await;
                match event {
                    Ok(op) => {
                        if let Some(filter) = endpoints.get(&op.endpoint_name) {
                            if filter::op_satisfies_filter(
                                &op,
                                filter.filter.as_ref(),
                                &filter.schema,
                            ) && (tx.send(event_mapper(op)).await).is_err()
                            {
                                // receiver dropped
                                break;
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
