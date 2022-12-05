use dozer_cache::cache::expression::QueryExpression;
use dozer_types::log::warn;
use dozer_types::serde_json;
use dozer_types::types::{Record, Schema};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tonic::{Code, Status};

use crate::{api_helper::ApiHelper, PipelineDetails};

use super::internal_grpc::PipelineRequest;

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

pub async fn on_event<T>(
    mut broadcast_receiver: Receiver<PipelineRequest>,
    tx: Sender<T>,
    event_mapper: impl Fn(PipelineRequest) -> Option<T>,
) {
    loop {
        let event = broadcast_receiver.recv().await;
        match event {
            Ok(event) => {
                if let Some(event) = event_mapper(event) {
                    if (tx.send(event).await).is_err() {
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
