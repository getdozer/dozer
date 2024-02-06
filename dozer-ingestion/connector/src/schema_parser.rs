use dozer_types::models::ingestion_types::ConfigSchemas;
use dozer_types::thiserror::{self, Error};
use std::path::{Path, PathBuf};

#[derive(Debug, Error)]
pub enum SchemaParserError {
    #[error("cannot read file {0:?}: {1}")]
    CannotReadFile(PathBuf, #[source] std::io::Error),
}

pub struct SchemaParser;

impl SchemaParser {
    pub fn parse_config(schemas: &ConfigSchemas) -> Result<String, SchemaParserError> {
        match schemas {
            ConfigSchemas::Inline(schemas_str) => Ok(schemas_str.clone()),
            ConfigSchemas::Path(path) => {
                let path = Path::new(path);
                std::fs::read_to_string(path)
                    .map_err(|e| SchemaParserError::CannotReadFile(path.to_path_buf(), e))
            }
        }
    }
}
