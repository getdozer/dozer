use crate::dag::dag::PortDirection::{Input, Output};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InvalidNodeHandle, InvalidNodeType, InvalidPortHandle};
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};

use std::collections::HashMap;
use std::sync::Arc;

pub const DEFAULT_PORT_HANDLE: u16 = 0xffff_u16;

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

pub enum NodeType {
    Source(Arc<dyn SourceFactory>),
    Processor(Arc<dyn ProcessorFactory>),
    Sink(Arc<dyn SinkFactory>),
}

pub struct Node {
    handle: NodeHandle,
    t: NodeType,
}

pub struct Dag {
    pub nodes: HashMap<NodeHandle, NodeType>,
    pub edges: Vec<Edge>,
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
            nodes: HashMap::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node_builder: NodeType, handle: NodeHandle) {
        self.nodes.insert(handle, node_builder);
    }

    fn get_ports(&self, n: &NodeType, d: PortDirection) -> Result<Vec<PortHandle>, ExecutionError> {
        match n {
            NodeType::Processor(p) => {
                if matches!(d, Output) {
                    Ok(p.get_output_ports().iter().map(|e| e.handle).collect())
                } else {
                    Ok(p.get_input_ports())
                }
            }
            NodeType::Sink(s) => {
                if matches!(d, Output) {
                    Err(InvalidNodeType)
                } else {
                    Ok(s.get_input_ports())
                }
            }
            NodeType::Source(s) => {
                if matches!(d, Output) {
                    Ok(s.get_output_ports().iter().map(|e| e.handle).collect())
                } else {
                    Err(InvalidNodeType)
                }
            }
        }
    }

    pub fn connect(&mut self, from: Endpoint, to: Endpoint) -> Result<(), ExecutionError> {
        let src_node = self.nodes.get(&from.node);
        if src_node.is_none() {
            return Err(InvalidNodeHandle(from.node));
        }

        let dst_node = self.nodes.get(&to.node);
        if dst_node.is_none() {
            return Err(InvalidNodeHandle(to.node));
        }

        let src_output_ports = self.get_ports(src_node.unwrap(), Output)?;
        if !src_output_ports.contains(&from.port) {
            return Err(InvalidPortHandle(from.port));
        }

        let dst_input_ports = self.get_ports(dst_node.unwrap(), Input)?;
        if !dst_input_ports.contains(&to.port) {
            return Err(InvalidPortHandle(to.port));
        }

        self.edges.push(Edge::new(from, to));
        Ok(())
    }

    pub fn merge(&mut self, ns: Option<u16>, other: Dag) {
        for (handle, node) in other.nodes {
            self.nodes.insert(
                NodeHandle::new(
                    if let Some(ns) = ns {
                        Some(ns)
                    } else {
                        handle.ns
                    },
                    handle.id,
                ),
                node,
            );
        }
        for edge in other.edges {
            self.edges.push(Edge::new(
                Endpoint::new(
                    NodeHandle::new(
                        if let Some(ns) = ns {
                            Some(ns)
                        } else {
                            edge.from.node.ns
                        },
                        edge.from.node.id,
                    ),
                    edge.from.port,
                ),
                Endpoint::new(
                    NodeHandle::new(
                        if let Some(ns) = ns {
                            Some(ns)
                        } else {
                            edge.to.node.ns
                        },
                        edge.to.node.id,
                    ),
                    edge.to.port,
                ),
            ));
        }
    }

    fn get_node_children(&self, handle: &NodeHandle) -> Vec<NodeHandle> {
        self.edges
            .iter()
            .filter(|e| &e.from.node == handle)
            .map(|e| e.to.node.clone())
            .collect()
    }

    pub fn get_sources(&self) -> Vec<(NodeHandle, &Arc<dyn SourceFactory>)> {
        let mut r: Vec<(NodeHandle, &Arc<dyn SourceFactory>)> = Vec::new();
        for (handle, typ) in &self.nodes {
            if let NodeType::Source(s) = typ {
                r.push((handle.clone(), s))
            }
        }
        r
    }

    pub fn get_processors(&self) -> Vec<(NodeHandle, &Arc<dyn ProcessorFactory>)> {
        let mut r: Vec<(NodeHandle, &Arc<dyn ProcessorFactory>)> = Vec::new();
        for (handle, typ) in &self.nodes {
            if let NodeType::Processor(s) = typ {
                r.push((handle.clone(), s));
            }
        }
        r
    }

    pub fn get_sinks(&self) -> Vec<(NodeHandle, &Arc<dyn SinkFactory>)> {
        let mut r: Vec<(NodeHandle, &Arc<dyn SinkFactory>)> = Vec::new();
        for (handle, typ) in &self.nodes {
            if let NodeType::Sink(s) = typ {
                r.push((handle.clone(), s));
            }
        }
        r
    }
}
