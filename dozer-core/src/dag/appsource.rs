use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    AmbiguousSourceIdentifier, AppSourceConnectionAlreadyExists, InvalidSourceIdentifier,
};
use crate::dag::node::{PortHandle, SourceFactory};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppSource<T> {
    pub connection: String,
    pub source: Arc<dyn SourceFactory<T>>,
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

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct AppSourceId {
    pub id: String,
    pub connection: Option<String>,
}

impl Display for AppSourceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let conn_str = if let Some(conn) = &self.connection {
            format!("{}.", conn.as_str())
        } else {
            "".to_string()
        };

        f.write_str(format!("{}{}", conn_str, self.id.as_str()).as_str())
    }
}

impl AppSourceId {
    pub fn new(id: String, connection: Option<String>) -> Self {
        Self { id, connection }
    }
}

pub struct AppSourceMappings<'a, T> {
    pub source: &'a AppSource<T>,
    pub mappings: HashMap<AppSourceId, PortHandle>,
}

impl<'a, T> AppSourceMappings<'a, T> {
    pub fn new(source: &'a AppSource<T>, mappings: HashMap<AppSourceId, PortHandle>) -> Self {
        Self { source, mappings }
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

    pub fn get(&self, ls: Vec<AppSourceId>) -> Result<Vec<AppSourceMappings<T>>, ExecutionError> {
        let mut res: HashMap<usize, HashMap<AppSourceId, PortHandle>> = HashMap::new();

        for source in ls {
            let found: Vec<(usize, PortHandle)> = self
                .sources
                .iter()
                .enumerate()
                .filter(|(_idx, s)| {
                    ((source.connection.is_some()
                        && source.connection.as_ref().unwrap() == &s.connection)
                        || source.connection.is_none())
                        && s.mappings.contains_key(&source.id)
                })
                .map(|s| (s.0, *s.1.mappings.get(&source.id).unwrap()))
                .collect();

            match (found.len(), &source.connection) {
                (0, _) => return Err(InvalidSourceIdentifier(source)),
                (1, _) => {
                    let (idx, port) = found.first().unwrap();
                    let entry = res.entry(*idx).or_default();
                    entry.insert(source, *port);
                }
                (_, None) => return Err(AmbiguousSourceIdentifier(source)),
                (_, Some(conn)) => {
                    let found: Vec<(usize, PortHandle)> = self
                        .sources
                        .iter()
                        .enumerate()
                        .filter(|(_idx, s)| {
                            &s.connection == conn && s.mappings.contains_key(&source.id)
                        })
                        .map(|s| (s.0, *s.1.mappings.get(&source.id).unwrap()))
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
            }
        }

        Ok(res
            .into_iter()
            .map(|(idx, map)| AppSourceMappings::new(&self.sources[idx], map))
            .collect())
    }
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }
}
