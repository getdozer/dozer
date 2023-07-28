use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{
    AmbiguousSourceIdentifier, AppSourceConnectionAlreadyExists, InvalidSourceIdentifier,
};
use crate::node::{PortHandle, SourceFactory};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppSource<T> {
    pub connection: String,
    pub source: Arc<dyn SourceFactory<T>>,
    /// From source name to output port handle.
    pub mappings: HashMap<String, PortHandle>,
}

impl<T> AppSource<T> {
    pub fn new(
        connection: String,
        source: Arc<dyn SourceFactory<T>>,
        mappings: HashMap<String, PortHandle>,
    ) -> Self {
        Self {
            connection,
            source,
            mappings,
        }
    }
}

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

pub struct AppSourceManager<T> {
    pub(crate) sources: Vec<AppSource<T>>,
}

impl<T> Default for AppSourceManager<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AppSourceManager<T> {
    pub fn add(&mut self, src: AppSource<T>) -> Result<(), ExecutionError> {
        if self.sources.iter().any(|s| s.connection == src.connection) {
            return Err(AppSourceConnectionAlreadyExists(src.connection));
        }

        self.sources.push(src);
        Ok(())
    }
    pub fn get_sources(&self) -> Vec<(String, Arc<dyn SourceFactory<T>>)> {
        let mut sources = vec![];
        for source in &self.sources {
            sources.push((source.connection.clone(), source.source.clone()));
        }
        sources
    }
    pub fn get(&self, source_names: Vec<String>) -> Result<Vec<AppSourceMappings>, ExecutionError> {
        let mut res: HashMap<usize, HashMap<String, PortHandle>> = HashMap::new();

        for source in source_names {
            let found: Vec<(usize, PortHandle)> = self
                .sources
                .iter()
                .enumerate()
                .filter_map(|(index, app_source)| {
                    app_source
                        .mappings
                        .get(&source)
                        .map(|output_port| (index, *output_port))
                })
                .collect();

            match found.len() {
                0 => return Err(InvalidSourceIdentifier(source)),
                1 => {
                    let (idx, port) = found.first().unwrap();
                    let entry = res.entry(*idx).or_default();
                    entry.insert(source, *port);
                }
                _ => return Err(AmbiguousSourceIdentifier(source)),
            }
        }

        Ok(res
            .into_iter()
            .map(|(idx, map)| AppSourceMappings::new(self.sources[idx].connection.clone(), map))
            .collect())
    }
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }
}
