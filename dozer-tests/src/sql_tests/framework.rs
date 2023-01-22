use super::pipeline::TestPipeline;
use super::{helper::*, SqlMapper};

use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use dozer_types::types::{Record, Schema};

use std::sync::{Arc, Mutex};

#[derive(Error, Debug)]
pub enum FrameworkError {
    #[error(transparent)]
    InternalError(#[from] BoxedError),
}

#[derive(Default)]
pub struct TestFramework {
    pub source: Arc<Mutex<SqlMapper>>,
    pub dest: Arc<Mutex<SqlMapper>>,
}
pub struct QueryResult {
    pub output_schema: Schema,
    pub records: Vec<Record>,
}

impl TestFramework {
    // SQLite as initialized as a sink with data flowing throwing SQL pipeline
    pub fn query(
        &mut self,
        list: Vec<(&'static str, String)>,
        final_sql: String,
    ) -> Result<QueryResult, FrameworkError> {
        let source_schema_map = self.source.lock().unwrap().schema_map.clone();

        let ops = self
            .source
            .lock()
            .unwrap()
            .execute_list(list)
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        let mut pipeline =
            TestPipeline::new(final_sql.clone(), source_schema_map, ops, self.dest.clone());

        let output_schema = pipeline.get_schema();

        pipeline
            .run()
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        let source_result = query_sqlite(self.source.clone(), &final_sql, &output_schema)
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;
        Ok(QueryResult {
            output_schema,
            records: source_result,
        })
    }

    // Compare Source and Dest SQLlite records.
    pub fn compare_with_sqlite(
        &mut self,
        list: Vec<(&'static str, String)>,
        final_sql: String,
    ) -> Result<bool, FrameworkError> {
        let query_result = self
            .query(list, final_sql)
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        let dest_result = query_sqlite(
            self.dest.clone(),
            "select * from results;",
            &query_result.output_schema,
        )
        .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        Ok(query_result.records == dest_result)
    }
}
