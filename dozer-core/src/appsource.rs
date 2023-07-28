use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{AmbiguousSourceIdentifier, InvalidSourceIdentifier};
use crate::node::{PortHandle, SourceFactory};
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

#[derive(Debug)]
pub struct AppSourceManager<T> {
    pub(crate) sources: Vec<Box<dyn SourceFactory<T>>>,
    pub(crate) mappings: Vec<AppSourceMappings>,
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
    pub fn add(&mut self, source: Box<dyn SourceFactory<T>>, mapping: AppSourceMappings) {
        self.sources.push(source);
        self.mappings.push(mapping);
    }

    pub fn get_endpoint(&self, source_name: &str) -> Result<(&str, PortHandle), ExecutionError> {
        get_endpoint_from_mappings(&self.mappings, source_name)
    }

    pub fn new() -> Self {
        Self::default()
    }
}

/// Returns the (connection name, output port) that outputs the table with source name `source_name`.
pub fn get_endpoint_from_mappings<'a>(
    mappings: &'a [AppSourceMappings],
    source_name: &str,
) -> Result<(&'a str, PortHandle), ExecutionError> {
    let mut found: Vec<(&str, PortHandle)> = mappings
        .iter()
        .filter_map(|mapping| {
            mapping
                .mappings
                .get(source_name)
                .map(|output_port| (mapping.connection.as_str(), *output_port))
        })
        .collect();

    match found.len() {
        0 => Err(InvalidSourceIdentifier(source_name.to_string())),
        1 => Ok(found.remove(0)),
        _ => Err(AmbiguousSourceIdentifier(source_name.to_string())),
    }
}
