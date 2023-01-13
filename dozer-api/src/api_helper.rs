use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use crate::generator::oapi::generator::OpenApiGenerator;
use crate::PipelineDetails;
use dozer_cache::cache::{expression::QueryExpression, index};
use dozer_cache::errors::CacheError;
use dozer_cache::{AccessFilter, CacheReader};
use dozer_types::indexmap::IndexMap;
use dozer_types::json_str_to_field;
use dozer_types::record_to_map;
use dozer_types::serde_json::Value;
use dozer_types::types::{Record, Schema};
use openapiv3::OpenAPI;

pub struct ApiHelper<'a> {
    details: &'a PipelineDetails,
    reader: CacheReader,
}
impl<'a> ApiHelper<'a> {
    pub fn new(
        pipeline_details: &'a PipelineDetails,
        access: Option<Access>,
    ) -> Result<Self, ApiError> {
        let access = access.unwrap_or(Access::All);

        // Define Access Filter based on token
        let reader_access = match access {
            // No access filter.
            Access::All => AccessFilter {
                filter: None,
                fields: vec![],
            },

            Access::Custom(mut access_filters) => {
                if let Some(access_filter) = access_filters.remove(&pipeline_details.schema_name) {
                    access_filter
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
        let (schema, secondary_indexes) = self
            .reader
            .get_schema_and_indexes_by_name(&schema_name)
            .map_err(ApiError::SchemaNotFound)?;

        let oapi_generator = OpenApiGenerator::new(
            schema,
            secondary_indexes,
            schema_name,
            self.details.cache_endpoint.endpoint.clone(),
            vec![format!("http://localhost:{}", "8080")],
        );

        oapi_generator
            .generate_oas3()
            .map_err(ApiError::ApiGenerationError)
    }

    /// Get a single record by json string as primary key
    pub fn get_record(&self, key: &str) -> Result<IndexMap<String, Value>, CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(&self.details.schema_name)?
            .0;

        if schema.primary_index.len() != 1 {
            todo!("Decide what to do when primary key is empty or has more than one field");
        }

        let field = &schema.fields[schema.primary_index[0]];
        let key =
            json_str_to_field(key, field.typ, field.nullable).map_err(CacheError::TypeError)?;

        let key = index::get_primary_key(&[0], &[key]);
        let rec = self.reader.get(&key)?;

        record_to_map(&rec, &schema).map_err(CacheError::TypeError)
    }

    pub fn get_records_count(&self, mut exp: QueryExpression) -> Result<usize, CacheError> {
        self.reader.count(&self.details.schema_name, &mut exp)
    }

    /// Get multiple records
    pub fn get_records_map(
        &self,
        exp: QueryExpression,
    ) -> Result<Vec<IndexMap<String, Value>>, CacheError> {
        let mut maps = vec![];
        let (schema, records) = self.get_records(exp)?;
        for rec in records.iter() {
            let map = record_to_map(rec, &schema)?;
            maps.push(map);
        }
        Ok(maps)
    }
    /// Get multiple records
    pub fn get_records(
        &self,
        mut exp: QueryExpression,
    ) -> Result<(Schema, Vec<Record>), CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(&self.details.schema_name)?
            .0;
        let records = self.reader.query(&self.details.schema_name, &mut exp)?;

        Ok((schema, records))
    }

    /// Get schema
    pub fn get_schema(&self) -> Result<Schema, CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(&self.details.schema_name)?
            .0;
        Ok(schema)
    }
}
