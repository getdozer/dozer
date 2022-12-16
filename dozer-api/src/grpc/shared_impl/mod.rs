use dozer_cache::cache::expression::QueryExpression;
use dozer_types::log::warn;
use dozer_types::serde_json;
use dozer_types::types::{Record, Schema};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Response, Status};

use crate::{api_helper::ApiHelper, PipelineDetails};

use super::internal_grpc::pipeline_response::ApiEvent;
use super::internal_grpc::PipelineResponse;
use super::types::Operation;

mod filter;

pub fn from_error(error: impl std::error::Error) -> Status {
    Status::new(Code::Internal, error.to_string())
}

pub fn query(
    pipeline_details: PipelineDetails,
    query: Option<&str>,
) -> Result<(Schema, Vec<Record>), Status> {
    let query = match query {
        Some(query) => {
            if query.is_empty() {
                QueryExpression::default()
            } else {
                serde_json::from_str(query).map_err(from_error)?
            }
        }
        None => QueryExpression::default(),
    };
    let api_helper = ApiHelper::new(pipeline_details, None)?;
    let (schema, records) = api_helper.get_records(query).map_err(from_error)?;
    Ok((schema, records))
}

pub fn on_event<T: Send + 'static>(
    pipeline_details: PipelineDetails,
    filter: Option<&str>,
    mut broadcast_receiver: Receiver<PipelineResponse>,
    event_mapper: impl Fn(Operation, String) -> Option<T> + Send + Sync + 'static,
) -> Result<Response<ReceiverStream<T>>, Status> {
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

    let api_helper = ApiHelper::new(pipeline_details.clone(), None)?;
    let schema = api_helper
        .get_schema()
        .map_err(|_| Status::invalid_argument(&pipeline_details.cache_endpoint.endpoint.name))?;

    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        loop {
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
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}
