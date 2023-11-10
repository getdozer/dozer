pub mod api_config;
pub mod api_endpoint;
pub mod api_security;
pub mod app_config;
pub mod cloud;
pub mod config;
pub mod connection;
pub mod flags;
pub mod ingestion_types;
mod json_schema_helper;
pub mod lambda_config;
pub mod source;
pub mod telemetry;
pub mod udf_config;
pub use json_schema_helper::{get_connection_schemas, get_dozer_schema};

fn equal_default<T: PartialEq + Default>(t: &T) -> bool {
    t == &T::default()
}
