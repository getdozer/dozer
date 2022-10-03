use crate::dag::dag::PortDirection::{Input, Output};
use crate::dag::node::NextStep::Continue;
use crate::dag::node::{ExecutionContext, NextStep, Processor, ProcessorFactory, Sink, SinkFactory, Source, SourceFactory};
use dozer_types::types::{Operation, OperationEvent, Record, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;
use anyhow::{anyhow, Error};
use uuid::Uuid;
use crate::dag::mt_executor::DefaultPortHandle;
use crate::state::{StateStore, StateStoresManager};


pub type NodeHandle = u16;
pub type PortHandle = u16;

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
    pub to: Endpoint
}

impl Edge {
    pub fn new(from: Endpoint, to: Endpoint) -> Self {
        Self { from, to }
    }
}

pub enum NodeType {
    Source(Box<dyn SourceFactory>),
    Sink(Box<dyn SinkFactory>),
    Processor(Box<dyn ProcessorFactory>),
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

    fn get_ports(&self, n: &NodeType, d: PortDirection) -> anyhow::Result<Vec<PortHandle>> {
        match n {
            NodeType::Processor(p) => {
                if matches!(d, Output) {
                    Ok(p.get_output_ports())
                } else {
                    Ok(p.get_input_ports())
                }
            }
            NodeType::Sink(s) => {
                if matches!(d, Output) {
                    Err(anyhow!("Invalid node type"))
                } else {
                    Ok(s.get_input_ports())
                }
            }
            NodeType::Source(s) => {
                if matches!(d, Output) {
                    Ok(s.get_output_ports())
                } else {
                    Err(anyhow!("Invalid node type"))
                }
            }
        }
    }

    pub fn connect(
        &mut self,
        from: Endpoint,
        to: Endpoint
    ) -> anyhow::Result<()> {

        let src_node = self.nodes.get(&from.node);
        if src_node.is_none() {
            return Err(anyhow!(
                "Unable to find source node with id = {}",
                from.node.to_string()
            ));
        }

        let dst_node = self.nodes.get(&to.node);
        if dst_node.is_none() {
            return Err(anyhow!(
                "Unable to find source node with id = {}",
                to.node.to_string()
            ));
        }

        let src_output_ports = self.get_ports(src_node.unwrap(), Output)?;
        if !src_output_ports.contains(&from.port) {
            return Err(anyhow!("Unable to connect port"));
        }

        let dst_input_ports = self.get_ports(dst_node.unwrap(), Input)?;
        if !dst_input_ports.contains(&to.port) {
            return Err(anyhow!("Unable to connect port"));
        }

        self.edges.push(Edge::new(from, to));
        Ok(())
    }
}
