pub mod models;
pub mod pipeline;
mod services;
pub mod simple;
use dozer_types::types::{Schema};

use crate::models::{connection::Connection, endpoint::Endpoint, source::Source};

pub trait Orchestrator {
    fn test_connection(input: Connection) -> anyhow::Result<()>;
    fn get_schema(input: Connection) -> anyhow::Result<Vec<(String, Schema)>>;
    fn sql(&mut self, sql: String) -> &mut Self;
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self;
    fn add_endpoints(&mut self, endpoints: Vec<Endpoint>) -> &mut Self;
    fn run(&mut self) -> anyhow::Result<&mut Self>;
}
