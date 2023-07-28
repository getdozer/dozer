use dozer_types::node::NodeHandle;

use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{
    AmbiguousSourceIdentifier, AppSourceConnectionAlreadyExists, InvalidSourceIdentifier,
};
use crate::node::{PortHandle, SourceFactory};
use crate::Endpoint;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct AppSourceMappings {
    pub connection: String,
    /// From source name to input port handle.
    pub mappings: HashMap<String, PortHandle>,
}

impl AppSourceMappings {
    pub fn new(connection: String, mappings: HashMap<String, PortHandle>) -> Self {
        Self {
            connection,
            mappings,
        }
    }
}

#[derive(Debug)]
pub struct AppSourceManager<T> {
    sources: Vec<Arc<dyn SourceFactory<T>>>,
    mappings: Vec<AppSourceMappings>,
}

impl<T> Default for AppSourceManager<T> {
    fn default() -> Self {
        Self {
            sources: vec![],
            mappings: vec![],
        }
    }
}

impl<T> AppSourceManager<T> {
    pub fn add(
        &mut self,
        source: Arc<dyn SourceFactory<T>>,
        mapping: AppSourceMappings,
    ) -> Result<(), ExecutionError> {
        if self
            .mappings
            .iter()
            .any(|existing_mapping| existing_mapping.connection == mapping.connection)
        {
            return Err(AppSourceConnectionAlreadyExists(mapping.connection));
        }

        self.sources.push(source);
        self.mappings.push(mapping);
        Ok(())
    }
    pub fn get_sources(&self) -> Vec<(String, Arc<dyn SourceFactory<T>>)> {
        let mut sources = vec![];
        for (source, mapping) in self.sources.iter().zip(&self.mappings) {
            sources.push((mapping.connection.clone(), source.clone()));
        }
        sources
    }
    pub fn get_endpoint(&self, source_name: &str) -> Result<Endpoint, ExecutionError> {
        let mut found: Vec<Endpoint> = self
            .mappings
            .iter()
            .filter_map(|mapping| {
                mapping.mappings.get(source_name).map(|output_port| {
                    Endpoint::new(
                        NodeHandle::new(None, mapping.connection.clone()),
                        *output_port,
                    )
                })
            })
            .collect();

        match found.len() {
            0 => Err(InvalidSourceIdentifier(source_name.to_string())),
            1 => Ok(found.remove(0)),
            _ => Err(AmbiguousSourceIdentifier(source_name.to_string())),
        }
    }

    pub fn new() -> Self {
        Self::default()
    }
}
