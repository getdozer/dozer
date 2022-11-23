use crate::dag::dag::PortDirection::{Input, Output};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InvalidNodeHandle, InvalidNodeType, InvalidPortHandle};
use crate::dag::node::{
    NodeHandle, PortHandle, StatefulProcessorFactory, StatefulSinkFactory, StatefulSourceFactory,
    StatelessProcessorFactory, StatelessSinkFactory, StatelessSourceFactory,
};
use std::collections::HashMap;

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
    StatelessSource(Box<dyn StatelessSourceFactory>),
    StatefulSource(Box<dyn StatefulSourceFactory>),
    StatelessProcessor(Box<dyn StatelessProcessorFactory>),
    StatefulProcessor(Box<dyn StatefulProcessorFactory>),
    StatelessSink(Box<dyn StatelessSinkFactory>),
    StatefulSink(Box<dyn StatefulSinkFactory>),
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
            NodeType::StatelessProcessor(p) => {
                if matches!(d, Output) {
                    Ok(p.get_output_ports())
                } else {
                    Ok(p.get_input_ports())
                }
            }
            NodeType::StatefulProcessor(p) => {
                if matches!(d, Output) {
                    Ok(p.get_output_ports().iter().map(|e| e.handle).collect())
                } else {
                    Ok(p.get_input_ports())
                }
            }
            NodeType::StatelessSink(s) => {
                if matches!(d, Output) {
                    Err(InvalidNodeType)
                } else {
                    Ok(s.get_input_ports())
                }
            }
            NodeType::StatefulSink(s) => {
                if matches!(d, Output) {
                    Err(InvalidNodeType)
                } else {
                    Ok(s.get_input_ports())
                }
            }
            NodeType::StatelessSource(s) => {
                if matches!(d, Output) {
                    Ok(s.get_output_ports())
                } else {
                    Err(InvalidNodeType)
                }
            }
            NodeType::StatefulSource(s) => {
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

    pub fn merge(&mut self, namespace: String, other: Dag) {
        for node in other.nodes {
            self.nodes
                .insert(format!("{}_{}", namespace, node.0), node.1);
        }

        for edge in other.edges {
            self.edges.push(Edge::new(
                Endpoint::new(format!("{}_{}", namespace, edge.from.node), edge.from.port),
                Endpoint::new(format!("{}_{}", namespace, edge.to.node), edge.to.port),
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

    pub fn get_sources(&self) -> Vec<NodeHandle> {
        self.nodes
            .iter()
            .filter(|source| {
                matches!(source.1, NodeType::StatefulSource(_))
                    || matches!(source.1, NodeType::StatelessSource(_))
            })
            .map(|e| e.0.clone())
            .collect()
    }

    pub fn is_stateful(&self, handle: &NodeHandle) -> Result<bool, ExecutionError> {
        let node = self
            .nodes
            .get(handle)
            .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

        Ok(match node {
            NodeType::StatelessProcessor(_) => true,
            NodeType::StatefulSource(_) => true,
            NodeType::StatefulSink(_) => true,
            _ => false,
        })
    }
}
