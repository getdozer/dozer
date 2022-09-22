use dozer_shared::types::TableInfo;
use std::error::Error;

use super::{
    models::{connection::Connection, endpoint::Endpoint, source::Source},
    services::connection::ConnectionService,
};

pub struct Dozer {
    sources: Option<Vec<Source>>,
    endpoints: Option<Vec<Endpoint>>,
}

impl Dozer {
    pub fn test_connection(input: Connection) -> Result<(), Box<dyn Error>> {
        let connection_service = ConnectionService::new(input);
        return connection_service.test_connection();
    }

    pub fn get_schema(input: Connection) -> Result<Vec<TableInfo>, Box<dyn Error>> {
        let connection_service = ConnectionService::new(input);
        return connection_service.get_schema();
    }
}

impl Dozer {
    pub fn new() -> Self {
        Self {
            sources: None,
            endpoints: None,
        }
    }
    pub fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        let my_source = self.sources.clone();
        match my_source {
            Some(current_data) => {
                let new_source = [current_data, sources.clone()].concat();
                self.sources = Some(new_source);
                return self;
            }
            None => {
                self.sources = Some(sources);
                return self;
            }
        }
    }
    pub fn add_endpoints(&mut self, endpoints: Vec<Endpoint>) -> &mut Self {
        let my_endpoint = self.endpoints.clone();
        match my_endpoint {
            Some(current_data) => {
                let new_endponts = [current_data, endpoints.clone()].concat();
                self.endpoints = Some(new_endponts);
                return self;
            }
            None => {
                self.endpoints = Some(endpoints);
                return self;
            }
        }
    }

    pub fn run(&mut self) -> Result<&mut Self, Box<dyn Error>> {
        todo!()
    }
}
