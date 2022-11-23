use super::pipeline::TestPipeline;
use super::{helper::*, SqlMapper};

use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

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

impl TestFramework {
    pub fn run_test(
        &mut self,
        list: Vec<(&str, String)>,
        final_sql: String,
    ) -> Result<bool, FrameworkError> {
        let name = list[0].0.clone();

        let source_schema = self.source.lock().unwrap().get_schema(name).to_owned();

        let ops = self
            .source
            .lock()
            .unwrap()
            .execute_list(list)
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        let mut pipeline =
            TestPipeline::new(final_sql.clone(), source_schema, ops, self.dest.clone());

        let output_schema = pipeline
            .run()
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        let source_result = query_sqllite(self.source.clone(), &final_sql, &output_schema)
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;
        let dest_result = query_sqllite(self.dest.clone(), "select * from results", &output_schema)
            .map_err(|e| FrameworkError::InternalError(Box::new(e)))?;

        Ok(source_result == dest_result)
    }
}
