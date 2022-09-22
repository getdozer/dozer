use std::error::Error;
use super::models::{endpoint::Endpoint, source::Source};

pub struct Dozer {
    sources: Option<Vec<Source>>,
    endpoints: Option<Vec<Endpoint>>,
}

impl Dozer {
    pub fn new() -> Self {
        Self { sources: None, endpoints: None }
    }
    pub fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        let my_source = self.sources.clone();
        match my_source {
            Some(current_data) => {
                let new_source =  [current_data, sources.clone()].concat();
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
                let new_endponts =  [current_data, endpoints.clone()].concat();
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
