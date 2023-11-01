use std::sync::Arc;

use actix_web::web::ReqData;
use actix_web::{web, HttpResponse};
use dozer_cache::cache::expression::{QueryExpression, Skip};
use dozer_cache::cache::CacheRecord;
use dozer_cache::{CacheReader, Phase};
use dozer_types::errors::types::CannotConvertF64ToJson;
use dozer_types::indexmap::IndexMap;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::types::{Field, Schema};
use openapiv3::OpenAPI;

use crate::api_helper::{get_record, get_records, get_records_count};
use crate::generator::oapi::generator::OpenApiGenerator;
use crate::CacheEndpoint;
use crate::{auth::Access, errors::ApiError};
use dozer_services::health::health_check_response::ServingStatus;
use dozer_types::json_types::field_to_json_value;
use dozer_types::serde_json::{json, Value};

use self::extractor::QueryExpressionExtractor;

fn generate_oapi3(reader: &CacheReader, endpoint: ApiEndpoint) -> Result<OpenAPI, ApiError> {
    let (schema, secondary_indexes) = reader.get_schema();

    let oapi_generator = OpenApiGenerator::new(
        schema,
        secondary_indexes,
        endpoint,
        vec![format!("http://localhost:{}", "8080")],
    );

    Ok(oapi_generator.generate_oas3())
}

/// Generated function to return openapi.yaml documentation.
pub async fn generate_oapi(
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
) -> Result<HttpResponse, ApiError> {
    generate_oapi3(
        &cache_endpoint.cache_reader(),
        cache_endpoint.endpoint.clone(),
    )
    .map(|result| HttpResponse::Ok().json(result))
}

// Generated Get function to return a single record in JSON format
pub async fn get(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let cache_reader = &cache_endpoint.cache_reader();
    let schema = &cache_reader.get_schema().0;

    let key = path.as_str();
    let key = if schema.primary_index.is_empty() {
        return Err(ApiError::NoPrimaryKey);
    } else if schema.primary_index.len() == 1 {
        let field = &schema.fields[schema.primary_index[0]];
        Field::from_str(key, field.typ, field.nullable).map_err(ApiError::InvalidPrimaryKey)?
    } else {
        return Err(ApiError::MultiIndexFetch(key.to_string()));
    };

    // This implementation must be consistent with `dozer_cache::cache::index::get_primary_key`
    let key = key.encode();
    let record = get_record(
        &cache_endpoint.cache_reader(),
        &key,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;

    record_to_map(record, schema)
        .map(|map| HttpResponse::Ok().json(map))
        .map_err(Into::into)
}

// Generated list function for multiple records with a default query expression
pub async fn list(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
) -> Result<HttpResponse, ApiError> {
    let mut exp = QueryExpression::new(None, vec![], Some(50), Skip::Skip(0));
    get_records_map(access, cache_endpoint, &mut exp).map(|map| HttpResponse::Ok().json(map))
}

// Generated get function for health check
pub async fn health_route() -> Result<HttpResponse, ApiError> {
    let status = ServingStatus::Serving;
    let resp = json!({ "status": status.as_str_name() }).to_string();
    Ok(HttpResponse::Ok().body(resp))
}

pub async fn count(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    query_expression: QueryExpressionExtractor,
) -> Result<HttpResponse, ApiError> {
    let mut query_expression = query_expression.0;
    get_records_count(
        &cache_endpoint.cache_reader(),
        &mut query_expression,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )
    .map(|count| HttpResponse::Ok().json(count))
}

// Generated query function for multiple records
pub async fn query(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    query_expression: QueryExpressionExtractor,
    default_max_num_records: web::Data<usize>,
) -> Result<HttpResponse, ApiError> {
    let mut query_expression = query_expression.0;
    if query_expression.limit.is_none() {
        query_expression.limit = Some(**default_max_num_records);
    }

    get_records_map(access, cache_endpoint, &mut query_expression)
        .map(|maps| HttpResponse::Ok().json(maps))
}

/// Get multiple records
fn get_records_map(
    access: Option<ReqData<Access>>,
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
    exp: &mut QueryExpression,
) -> Result<Vec<IndexMap<String, Value>>, ApiError> {
    let mut maps = vec![];
    let cache_reader = &cache_endpoint.cache_reader();
    let records = get_records(
        cache_reader,
        exp,
        &cache_endpoint.endpoint.name,
        access.map(|a| a.into_inner()),
    )?;
    let schema = &cache_reader.get_schema().0;
    for record in records.into_iter() {
        let map = record_to_map(record, schema)?;
        maps.push(map);
    }
    Ok(maps)
}

/// Used in REST APIs for converting to JSON
fn record_to_map(
    record: CacheRecord,
    schema: &Schema,
) -> Result<IndexMap<String, Value>, CannotConvertF64ToJson> {
    let mut map = IndexMap::new();

    for (field_def, field) in schema.fields.iter().zip(record.record.values) {
        let val = field_to_json_value(field)?;
        map.insert(field_def.name.clone(), val);
    }

    map.insert("__dozer_record_id".to_string(), Value::from(record.id));
    map.insert(
        "__dozer_record_version".to_string(),
        Value::from(record.version),
    );

    Ok(map)
}

pub async fn get_phase(
    cache_endpoint: ReqData<Arc<CacheEndpoint>>,
) -> Result<web::Json<Phase>, ApiError> {
    let cache_reader = cache_endpoint.cache_reader();
    let phase = cache_reader.get_phase().map_err(ApiError::GetPhaseFailed)?;
    Ok(web::Json(phase))
}

mod extractor {
    use std::{
        future::{ready, Ready},
        task::Poll,
    };

    use actix_http::{HttpMessage, Payload};
    use actix_web::{
        error::{ErrorBadRequest, JsonPayloadError},
        Error, FromRequest, HttpRequest,
    };
    use dozer_cache::cache::expression::QueryExpression;
    use dozer_types::serde_json;
    use futures_util::{future::Either, Future};
    use pin_project::pin_project;

    pub struct QueryExpressionExtractor(pub QueryExpression);

    impl FromRequest for QueryExpressionExtractor {
        type Error = Error;
        type Future =
            Either<Ready<Result<QueryExpressionExtractor, Error>>, QueryExpressionExtractFuture>;

        fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
            if let Err(e) = check_content_type(req) {
                Either::Left(ready(Err(e)))
            } else {
                Either::Right(QueryExpressionExtractFuture(String::from_request(
                    req, payload,
                )))
            }
        }
    }

    fn check_content_type(req: &HttpRequest) -> Result<(), Error> {
        if let Some(mime_type) = req.mime_type()? {
            if mime_type != "application/json" {
                return Err(ErrorBadRequest("Content-Type is not application/json"));
            }
        }
        Ok(())
    }
    type StringExtractFut = <String as FromRequest>::Future;

    #[pin_project]
    pub struct QueryExpressionExtractFuture(#[pin] StringExtractFut);

    impl Future for QueryExpressionExtractFuture {
        type Output = Result<QueryExpressionExtractor, Error>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let this = self.project();
            match this.0.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Ready(Ok(body)) => {
                    Poll::Ready(parse_query_expression(&body).map(QueryExpressionExtractor))
                }
            }
        }
    }

    fn parse_query_expression(body: &str) -> Result<QueryExpression, Error> {
        if body.is_empty() {
            return Ok(QueryExpression::with_no_limit());
        }

        serde_json::from_str(body)
            .map_err(JsonPayloadError::Deserialize)
            .map_err(Into::into)
    }
}
