use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use actix_web::web;
use actix_web::web::ReqData;
use dozer_cache::cache::{expression::QueryExpression, index, LmdbCache};
use dozer_cache::{AccessFilter, CacheReader};
use dozer_types::errors::cache::CacheError;
use dozer_types::json_value_to_field;
use dozer_types::record_to_json;
use dozer_types::types::FieldType;
use openapiv3::OpenAPI;
use std::{collections::HashMap, sync::Arc};

use crate::api_server::PipelineDetails;
use crate::generator::oapi::generator::OpenApiGenerator;

pub struct ApiHelper {
    details: ReqData<PipelineDetails>,
    reader: CacheReader,
}
impl ApiHelper {
    pub fn new(
        pipeline_details: ReqData<PipelineDetails>,
        cache: web::Data<Arc<LmdbCache>>,
        access: Option<ReqData<Access>>,
    ) -> Result<Self, ApiError> {
        let access = access.map_or(Access::All, |a| a.into_inner());

        // Define Access Filter based on token
        let reader_access = match access {
            // No access filter.
            Access::All => AccessFilter {
                filter: None,
                fields: vec![],
            },

            Access::Custom(access_filters) => {
                if let Some(access_filter) = access_filters.get(&pipeline_details.schema_name) {
                    access_filter.to_owned()
                } else {
                    return Err(ApiError::ApiAuthError(AuthError::InvalidToken));
                }
            }
        };

        let reader = CacheReader {
            cache: cache.as_ref().to_owned(),
            access: reader_access,
        };
        Ok(Self {
            details: pipeline_details,
            reader,
        })
    }

    pub fn generate_oapi3(&self) -> Result<OpenAPI, ApiError> {
        let schema_name = self.details.schema_name.clone();
        let schema = self
            .reader
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
        let schema = self.reader.get_schema_by_name(&self.details.schema_name)?;

        let field_types: Vec<FieldType> = schema
            .primary_index
            .iter()
            .map(|idx| schema.fields[*idx].typ.clone())
            .collect();
        let key = json_value_to_field(&key, &field_types[0]).map_err(CacheError::TypeError)?;

        let key = index::get_primary_key(&[0], &[key]);
        let rec = self.reader.get(&key)?;

        record_to_json(&rec, &schema).map_err(CacheError::TypeError)
    }

    /// Get multiple records
    pub fn get_records(
        &self,
        mut exp: QueryExpression,
    ) -> Result<Vec<HashMap<String, String>>, CacheError> {
        let schema = self.reader.get_schema_by_name(&self.details.schema_name)?;
        let records = self.reader.query(&self.details.schema_name, &mut exp)?;

        let mut maps = vec![];
        for rec in records.iter() {
            let map = record_to_json(rec, &schema)?;
            maps.push(map);
        }
        Ok(maps)
    }
}
