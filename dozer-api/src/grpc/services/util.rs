use crate::grpc::util::from_dynamic_message_to_json;
use dozer_cache::{cache::expression::QueryExpression, errors::CacheError};
use dozer_types::serde_json;
use prost_reflect::DynamicMessage;
use tonic::{Code, Status};

pub fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}

pub fn convert_grpc_message_to_query_exp(input: DynamicMessage) -> Result<QueryExpression, Status> {
    let json_present = from_dynamic_message_to_json(input)?;
    let mut string_present = json_present.to_string();
    let key_replace = vec![
        "filter", "and", "limit", "skip", "order_by", "eq", "lt", "lte", "gt", "gte",
    ];
    key_replace.iter().for_each(|&key| {
        let from = format!("\"{}\"", key);
        let to = format!("\"${}\"", key);
        string_present = string_present.replace(&from, &to);
    });
    let query_expression: QueryExpression = serde_json::from_str(&string_present)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(query_expression)
}
