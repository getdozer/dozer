use std::collections::HashMap;

use crate::api_helper::{get_records, get_records_count};
use crate::auth::Access;
use dozer_cache::cache::expression::{default_limit_for_query, FilterExpression, QueryExpression};
use dozer_cache::cache::CacheRecord;
use dozer_cache::CacheReader;
use dozer_types::errors::types::TypeError;
use dozer_types::grpc_types::types::Operation;
use dozer_types::log::warn;
use dozer_types::serde_json::{self, from_str, from_value, Map, Value};
use dozer_types::types::Schema;
use prost_reflect::prost_types;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Response, Status};

mod filter;

pub fn from_error(error: impl std::error::Error) -> Status {
    Status::new(Code::Internal, error.to_string())
}

fn parse_query(
    query: Option<serde_json::Value>,
    default: impl FnOnce() -> QueryExpression,
) -> Result<QueryExpression, Status> {
    match query {
        Some(query) => from_value(query.to_owned()).map_err(from_error),
        None => Ok(default()),
    }
}
pub fn prost_value_to_value(pvalue: prost_types::Value) -> Result<Option<Value>, TypeError> {
    println!("pvalue: {:?}", pvalue);
    match pvalue.kind {
        Some(k) => match k {
            prost_types::value::Kind::NullValue(_) => Ok(None),
            prost_types::value::Kind::NumberValue(n) => {
                if n.fract() == 0.0 {
                    Ok(Some(Value::from(n as i64)))
                } else {
                    Ok(Some(Value::from(n)))
                }
            }
            prost_types::value::Kind::StringValue(s) => Ok(Some(Value::String(s))),
            prost_types::value::Kind::BoolValue(b) => Ok(Some(Value::Bool(b))),
            prost_types::value::Kind::StructValue(s) => {
                let mut m = Map::new();
                for (name, value) in s.fields {
                    match prost_value_to_value(value)? {
                        Some(v) => {
                            m.insert(name, v);
                        }
                        None => {
                            return Err(TypeError::QueryParsingError(format!(
                                "Invalid inner value for field {}",
                                name
                            )));
                        }
                    }
                }
                Ok(Some(Value::Object(m)))
            }
            prost_types::value::Kind::ListValue(l) => {
                let mut vec = Vec::new();
                for (idx, value) in l.values.iter().enumerate() {
                    match prost_value_to_value(value.clone())? {
                        Some(v) => {
                            vec.push(v);
                        }
                        None => {
                            return Err(TypeError::QueryParsingError(format!(
                                "Invalid inner value at index :{}",
                                idx
                            )));
                        }
                    }
                }
                Ok(Some(Value::Array(vec)))
            }
        },
        None => Ok(None),
    }
}

pub fn count(
    reader: &CacheReader,
    query: Option<serde_json::Value>,
    endpoint: &str,
    access: Option<Access>,
) -> Result<usize, Status> {
    let mut query = parse_query(query, QueryExpression::with_no_limit)?;
    Ok(get_records_count(reader, &mut query, endpoint, access)?)
}

pub fn query(
    reader: &CacheReader,
    query: Option<serde_json::Value>,
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
                    Some(from_str(filter))
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
