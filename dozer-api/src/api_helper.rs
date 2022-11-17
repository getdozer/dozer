use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use crate::generator::oapi::generator::OpenApiGenerator;
use crate::PipelineDetails;
use dozer_cache::cache::{expression::QueryExpression, index};
use dozer_cache::errors::CacheError;
use dozer_cache::{AccessFilter, CacheReader};
use dozer_types::indexmap::IndexMap;
use dozer_types::json_value_to_field;
use dozer_types::record_to_json;
use dozer_types::types::{FieldType, Record, Schema};
use openapiv3::OpenAPI;

pub struct ApiHelper {
    details: PipelineDetails,
    reader: CacheReader,
}
impl ApiHelper {
    pub fn new(
        pipeline_details: PipelineDetails,
        access: Option<Access>,
    ) -> Result<Self, ApiError> {
        let access = access.map_or(Access::All, |a| a);

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
            cache: pipeline_details.cache_endpoint.cache.clone(),
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
            self.details.cache_endpoint.endpoint.clone(),
            vec![format!("http://localhost:{}", "8080")],
        );

        oapi_generator
            .generate_oas3()
            .map_err(ApiError::ApiGenerationError)
    }
    /// Get a single record
    pub fn get_record(&self, key: String) -> Result<IndexMap<String, String>, CacheError> {
        let schema = self.reader.get_schema_by_name(&self.details.schema_name)?;

        let field_types: Vec<FieldType> = schema
            .primary_index
            .iter()
            .map(|idx| schema.fields[*idx].typ)
            .collect();
        let key = json_value_to_field(&key, &field_types[0]).map_err(CacheError::TypeError)?;

        let key = index::get_primary_key(&[0], &[key]);
        let rec = self.reader.get(&key)?;

        record_to_json(&rec, &schema).map_err(CacheError::TypeError)
    }

    /// Get multiple records
    pub fn get_records_map(
        &self,
        exp: QueryExpression,
    ) -> Result<Vec<IndexMap<String, String>>, CacheError> {
        let mut maps = vec![];
        let (schema, records) = self.get_records(exp)?;
        for rec in records.iter() {
            let map = record_to_json(rec, &schema)?;
            maps.push(map);
        }
        Ok(maps)
    }
    /// Get multiple records
    pub fn get_records(
        &self,
        mut exp: QueryExpression,
    ) -> Result<(Schema, Vec<Record>), CacheError> {
        let schema = self.reader.get_schema_by_name(&self.details.schema_name)?;
        let records = self.reader.query(&self.details.schema_name, &mut exp)?;

        Ok((schema, records))
    }

    /// Get schema
    pub fn get_schema(&self) -> Result<Schema, CacheError> {
        let schema = self.reader.get_schema_by_name(&self.details.schema_name)?;
        Ok(schema)
    }

    pub fn convert_record_to_json(
        &self,
        record: Record,
    ) -> Result<IndexMap<String, String>, CacheError> {
        let schema = self.reader.get_schema_by_name(&self.details.schema_name)?;
        record_to_json(&record, &schema).map_err(CacheError::TypeError)
    }
}
