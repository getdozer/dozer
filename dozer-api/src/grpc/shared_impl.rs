use dozer_cache::cache::expression::QueryExpression;
use dozer_types::serde_json;
use dozer_types::types::{Record, Schema};
use tonic::{Code, Status};

use crate::{api_helper::ApiHelper, PipelineDetails};

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
