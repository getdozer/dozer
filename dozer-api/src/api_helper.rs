use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use crate::generator::oapi::generator::OpenApiGenerator;
use dozer_cache::cache::RecordWithId;
use dozer_cache::cache::{expression::QueryExpression, index};
use dozer_cache::errors::CacheError;
use dozer_cache::{AccessFilter, CacheReader};
use dozer_types::indexmap::IndexMap;
use dozer_types::json_str_to_field;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::record_to_map;
use dozer_types::serde_json::Value;
use dozer_types::types::Schema;
use openapiv3::OpenAPI;

pub struct ApiHelper<'a> {
    reader: &'a CacheReader,
    access_filter: AccessFilter,
    endpoint: &'a ApiEndpoint,
}
impl<'a> ApiHelper<'a> {
    pub fn new(
        reader: &'a CacheReader,
        endpoint: &'a ApiEndpoint,
        access: Option<Access>,
    ) -> Result<Self, ApiError> {
        let access = access.unwrap_or(Access::All);

        // Define Access Filter based on token
        let access_filter = match access {
            // No access filter.
            Access::All => AccessFilter {
                filter: None,
                fields: vec![],
            },

            Access::Custom(mut access_filters) => {
                if let Some(access_filter) = access_filters.remove(&endpoint.name) {
                    access_filter
                } else {
                    return Err(ApiError::ApiAuthError(AuthError::InvalidToken));
                }
            }
        };

        Ok(Self {
            reader,
            access_filter,
            endpoint,
        })
    }

    pub fn generate_oapi3(&self) -> Result<OpenAPI, ApiError> {
        let (schema, secondary_indexes) = self
            .reader
            .get_schema_and_indexes_by_name(&self.endpoint.name)
            .map_err(ApiError::SchemaNotFound)?;

        let oapi_generator = OpenApiGenerator::new(
            schema,
            secondary_indexes,
            self.endpoint.clone(),
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
            .get_schema_and_indexes_by_name(&self.endpoint.name)?
            .0;

        let key = if schema.primary_index.is_empty() {
            json_str_to_field(key, dozer_types::types::FieldType::UInt, false)
                .map_err(CacheError::Type)
        } else if schema.primary_index.len() == 1 {
            let field = &schema.fields[schema.primary_index[0]];
            json_str_to_field(key, field.typ, field.nullable).map_err(CacheError::Type)
        } else {
            Err(CacheError::Query(
                dozer_cache::errors::QueryError::MultiIndexFetch(key.to_string()),
            ))
        }?;

        let key = index::get_primary_key(&[0], &[key]);
        let rec = self.reader.get(&key, &self.access_filter)?;

        record_to_map(&rec, &schema).map_err(CacheError::Type)
    }

    pub fn get_records_count(self, mut exp: QueryExpression) -> Result<usize, CacheError> {
        self.reader
            .count(&self.endpoint.name, &mut exp, self.access_filter)
    }

    /// Get multiple records
    pub fn get_records_map(
        self,
        exp: QueryExpression,
    ) -> Result<Vec<IndexMap<String, Value>>, CacheError> {
        let mut maps = vec![];
        let (schema, records) = self.get_records(exp)?;
        for rec in records.iter() {
            let map = record_to_map(&rec.record, &schema)?;
            maps.push(map);
        }
        Ok(maps)
    }
    /// Get multiple records
    pub fn get_records(
        self,
        mut exp: QueryExpression,
    ) -> Result<(Schema, Vec<RecordWithId>), CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(&self.endpoint.name)?
            .0;
        let records = self
            .reader
            .query(&self.endpoint.name, &mut exp, self.access_filter)?;

        Ok((schema, records))
    }

    /// Get schema
    pub fn get_schema(&self) -> Result<Schema, CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(&self.endpoint.name)?
            .0;
        Ok(schema)
    }
}
