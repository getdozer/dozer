pub mod models;
mod services;
pub mod simple;

use dozer_types::types::TableInfo;

use crate::models::{connection::Connection, endpoint::Endpoint, source::Source};

pub trait Orchestrator {
    fn test_connection(input: Connection) -> anyhow::Result<()>;
    fn get_schema(input: Connection) -> anyhow::Result<Vec<TableInfo>>;
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self;
    fn add_endpoints(&mut self, endpoints: Vec<Endpoint>) -> &mut Self;
    fn run(&mut self) -> anyhow::Result<&mut Self>;
}
