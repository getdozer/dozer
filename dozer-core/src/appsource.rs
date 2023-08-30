use dozer_types::node::NodeHandle;

use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{
    AmbiguousSourceIdentifier, AppSourceConnectionAlreadyExists, InvalidSourceIdentifier,
};
use crate::node::{PortHandle, SourceFactory};
use crate::Endpoint;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AppSourceMappings {
    pub connection: String,
    /// From source name to output port handle.
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

#[derive(Debug, Default)]
pub struct AppSourceManager {
    pub(crate) sources: Vec<Box<dyn SourceFactory>>,
    pub(crate) mappings: Vec<AppSourceMappings>,
}

impl AppSourceManager {
    pub fn add(
        &mut self,
        source: Box<dyn SourceFactory>,
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

    pub fn get_endpoint(&self, source_name: &str) -> Result<Endpoint, ExecutionError> {
        get_endpoint_from_mappings(&self.mappings, source_name)
    }

    pub fn new() -> Self {
        Self::default()
    }
}

pub fn get_endpoint_from_mappings(
    mappings: &[AppSourceMappings],
    source_name: &str,
) -> Result<Endpoint, ExecutionError> {
    let mut found: Vec<Endpoint> = mappings
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
