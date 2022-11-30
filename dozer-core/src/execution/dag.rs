use crate::execution::dag::PortDirection::{Input, Output};
use crate::execution::errors::ExecutionError;
use crate::execution::errors::ExecutionError::{
    EdgeAlreadyExists, InvalidNodeHandle, InvalidNodeType, InvalidPortHandle,
};
use crate::execution::processor::{NodeHandle, PortHandle, ProcessorFactory};
use crate::execution::schema::ExecutionSchema;
use crate::sources::subscriptions::{RelationUniqueName, SourceId, SourceName};
use dozer_types::types::Schema;
use std::collections::{HashMap, HashSet};

pub type OutputHandle = String;
pub type InputHandle = String;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Endpoint {
    pub node: NodeHandle,
    pub port: PortHandle,
}

impl Endpoint {
    pub fn new(node: NodeHandle, port: PortHandle) -> Self {
        Self { node, port }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    pub from: Endpoint,
    pub to: Endpoint,
}

impl Edge {
    pub fn new(from: Endpoint, to: Endpoint) -> Self {
        Self { from, to }
    }
}

pub struct Node {
    handle: NodeHandle,
    proc: Box<dyn ProcessorFactory>,
}

pub struct Dag {
    inputs: HashMap<InputHandle, (Endpoint, ExecutionSchema)>,
    outputs: HashMap<Endpoint, OutputHandle>,
    processors: HashMap<NodeHandle, Box<dyn ProcessorFactory>>,
    edges: Vec<Edge>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PortDirection {
    Input,
    Output,
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

impl Dag {
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            inputs: HashMap::new(),
            processors: HashMap::new(),
            outputs: HashMap::new(),
        }
    }

    pub fn add_input(&mut self, name: InputHandle, schema: &Schema, to: Endpoint) {
        self.inputs.insert(
            name.clone(),
            (to, ExecutionSchema::from_schema(name, schema)),
        );
    }

    pub fn add_processor(&mut self, handle: NodeHandle, processor: Box<dyn ProcessorFactory>) {
        self.processors.insert(handle, processor);
    }

    pub fn add_output(&mut self, from: Endpoint, handle: OutputHandle) {
        self.outputs.insert(from, handle);
    }

    pub fn connect(&mut self, from: Endpoint, to: Endpoint) -> Result<(), ExecutionError> {
        match (
            self.processors.get(&from.node),
            self.processors.get(&to.node),
        ) {
            (Some(src_node), Some(target_node)) => {
                match (
                    src_node.get_output_ports().contains(&from.port),
                    target_node.get_input_ports().contains(&to.port),
                ) {
                    (true, true) => {
                        let edge = Edge::new(from, to);
                        match self.edges.contains(&edge) {
                            false => {
                                self.edges.push(edge);
                                Ok(())
                            }
                            true => Err(EdgeAlreadyExists { edge }),
                        }
                    }
                    (true, false) => Err(InvalidPortHandle(to.port)),
                    _ => Err(InvalidPortHandle(from.port)),
                }
            }
            (Some(_), None) => Err(InvalidNodeHandle(to.node)),
            _ => Err(InvalidNodeHandle(from.node)),
        }
    }
}
