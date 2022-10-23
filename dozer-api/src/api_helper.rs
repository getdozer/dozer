use crate::errors::ApiError;
use actix_web::web;
use actix_web::web::ReqData;
use dozer_cache::cache::{expression::QueryExpression, index, Cache, LmdbCache};
use dozer_types::errors::cache::CacheError;
use dozer_types::json_value_to_field;
use dozer_types::record_to_json;
use dozer_types::types::{Field, FieldType};
use openapiv3::OpenAPI;
use std::{collections::HashMap, sync::Arc};

use crate::api_server::PipelineDetails;
use crate::generator::oapi::generator::OpenApiGenerator;

pub struct ApiHelper {
    details: ReqData<PipelineDetails>,
    cache: web::Data<Arc<LmdbCache>>,
}
impl ApiHelper {
    pub fn new(
        pipeline_details: Option<ReqData<PipelineDetails>>,
        cache: web::Data<Arc<LmdbCache>>,
    ) -> Result<Self, ApiError> {
        if let Some(details) = pipeline_details {
            Ok(Self { details, cache })
        } else {
            Err(ApiError::PipelineNotInitialized)
        }
    }

    pub fn generate_oapi3(&self) -> Result<OpenAPI, ApiError> {
        let schema_name = self.details.schema_name.clone();
        let schema = self
            .cache
            .get_schema_by_name(&schema_name)
            .map_err(ApiError::SchemaNotFound)?;

        let oapi_generator = OpenApiGenerator::new(
            schema,
            schema_name,
            self.details.endpoint.clone(),
            vec![format!("http://localhost:{}", "8080")],
        );

        oapi_generator
            .generate_oas3()
            .map_err(ApiError::ApiGenerationError)
    }
    /// Get a single record
    pub fn get_record(&self, key: String) -> Result<HashMap<String, String>, CacheError> {
        let schema = self.cache.get_schema_by_name(&self.details.schema_name)?;

        let field_types: Vec<FieldType> = schema
            .primary_index
            .iter()
            .map(|idx| schema.fields[*idx].typ.clone())
            .collect();
        let key = json_value_to_field(&key, &field_types[0]);

        let key: Field = match key {
            Ok(key) => key,
            Err(e) => {
                panic!("error : {:?}", e);
            }
        };
        let key = index::get_primary_key(&[0], &[key]);
        let rec = self.cache.get(&key)?;

        record_to_json(&rec, &schema).map_err(CacheError::TypeError)
    }

    /// Get multiple records
    pub fn get_records(
        &self,
        exp: QueryExpression,
    ) -> Result<Vec<HashMap<String, String>>, CacheError> {
        let schema = self.cache.get_schema_by_name(&self.details.schema_name)?;
        let records = self.cache.query(&self.details.schema_name, &exp)?;

        let mut maps = vec![];
        for rec in records.iter() {
            let map = record_to_json(rec, &schema)?;
            maps.push(map);
        }
        Ok(maps)
    }
}
