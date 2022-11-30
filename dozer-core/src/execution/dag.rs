use crate::execution::dag::PortDirection::{Input, Output};
use crate::execution::errors::ExecutionError;
use crate::execution::errors::ExecutionError::{
    EdgeAlreadyExists, InvalidEndpoint, InvalidNodeHandle, InvalidNodeType, InvalidPortHandle,
};
use crate::execution::processor::{NodeHandle, PortHandle, ProcessorFactory};
use crate::execution::schema::ExecutionSchema;
use crate::sources::subscriptions::{RelationUniqueName, SourceId, SourceName};
use dozer_types::types::Schema;
use std::collections::{HashMap, HashSet};

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
    inputs: HashMap<NodeHandle, (Endpoint, ExecutionSchema)>,
    outputs: HashMap<NodeHandle, Endpoint>,
    processors: HashMap<NodeHandle, Box<dyn ProcessorFactory>>,
    edges: Vec<Edge>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PortDirection {
    Input,
    Output,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub node_handle: NodeHandle,
    pub port_handle: PortHandle,
    pub direction: PortDirection,
}

impl SchemaKey {
    pub fn new(node_handle: NodeHandle, port_handle: PortHandle, direction: PortDirection) -> Self {
        Self {
            node_handle,
            port_handle,
            direction,
        }
    }
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

    pub fn add_input(
        &mut self,
        handle: NodeHandle,
        schema: &Schema,
        to: Endpoint,
    ) -> Result<(), ExecutionError> {
        match self
            .processors
            .iter()
            .find(|(h, p)| **h == to.node && p.get_input_ports().contains(&to.port))
        {
            Some(_) => {
                self.inputs.insert(
                    handle.clone(),
                    (to, ExecutionSchema::from_schema(handle, schema)),
                );
                Ok(())
            }
            _ => Err(InvalidEndpoint { e: to }),
        }
    }

    pub fn add_processor(&mut self, handle: NodeHandle, processor: Box<dyn ProcessorFactory>) {
        self.processors.insert(handle, processor);
    }

    pub fn add_output(&mut self, from: Endpoint, handle: NodeHandle) -> Result<(), ExecutionError> {
        match self
            .processors
            .iter()
            .find(|(h, p)| **h == from.node && p.get_output_ports().contains(&from.port))
        {
            Some(_) => {
                self.outputs.insert(handle, from);
                Ok(())
            }
            _ => Err(InvalidEndpoint { e: from }),
        }
    }

    pub fn connect_processors(
        &mut self,
        from: Endpoint,
        to: Endpoint,
    ) -> Result<(), ExecutionError> {
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

    // fn get_node_output_schemas(
    //     &self,
    //     handle: &NodeHandle,
    //     input_schemas: HashMap<PortHandle, ExecutionSchema>,
    // ) -> Result<Vec<(Option<PortHandle>, ExecutionSchema)>, ExecutionError> {
    //     match (self.inputs.get(handle), self.processors.get(handle)) {
    //         (Some((endpoint, schema)), None) => Ok(vec![(None, schema.clone()); 1]),
    //         (None, Some(p)) => Ok(p
    //             .get_output_ports()
    //             .iter()
    //             .map(|port| (port.clone(), p.get_output_schema(p.clone(), &input_schemas)))
    //             .collect()),
    //         _ => Err(ExecutionError::InvalidNodeHandle(handle)),
    //     }

    // match n {
    //     NodeType::Source(src) => {
    //         let mut schemas: HashMap<PortHandle, Schema> = HashMap::new();
    //         for p in src.get_output_ports() {
    //             schemas.insert(p, src.get_output_schema(p)?);
    //         }
    //         Ok(Some(schemas))
    //     }
    //     NodeType::Processor(proc) => {
    //         let mut schemas: HashMap<PortHandle, Schema> = HashMap::new();
    //         for p in proc.get_output_ports() {
    //             schemas.insert(
    //                 p,
    //                 proc.get_output_schema(p, input_schemas.clone().unwrap())?,
    //             );
    //         }
    //         Ok(Some(schemas))
    //     }
    //     NodeType::Sink(snk) => Ok(None),
    // }
    // }

    // fn index_node_schemas(
    //     &self,
    //     node_handle: NodeHandle,
    //     node_input_schemas: Option<HashMap<PortHandle, ExecutionSchema>>,
    //     dag: &Dag,
    //     res: &mut HashMap<SchemaKey, Schema>,
    // ) -> Result<(), ExecutionError> {
    //     /// Get all output schemas of the current node
    //     let output_schemas = self.get_node_output_schemas(node, node_input_schemas)?;
    //     /// If no output schemas, it's a SNK so nothing to do
    //     if output_schemas.is_none() {
    //         return Ok(());
    //     }
    //
    //     /// Iterate all output schemas for this node
    //     for s in output_schemas.unwrap() {
    //         /// Index (Node, Port, Output) -> Schema
    //         res.insert(
    //             SchemaKey::new(node_handle.clone(), s.0, PortDirection::Output),
    //             s.1.clone(),
    //         );
    //
    //         /// Now get all edges connecting this (Node,Port) to the next nodes
    //         let out_edges: Vec<Endpoint> = dag
    //             .edges
    //             .iter()
    //             .filter(|e| e.from.node == node_handle && e.from.port == s.0)
    //             .map(|e| (e.to.clone()))
    //             .collect();
    //
    //         if out_edges.len() == 0 {
    //             return Err(anyhow!(
    //                 "The output port {} of the node {} is not connected",
    //                 s.0,
    //                 node_handle
    //             ));
    //         }
    //
    //         for out_e in out_edges {
    //             /// Register the target of the edge (Node, Port, Input) -> Schema
    //             res.insert(
    //                 SchemaKey::new(out_e.node.clone(), out_e.port, PortDirection::Input),
    //                 s.1.clone(),
    //             );
    //
    //             /// find the next node in teh chain
    //             let next_node_handle = out_e.node.clone();
    //             let next_node = dag
    //                 .nodes
    //                 .get(&out_e.node.clone())
    //                 .context(anyhow!("Unable to find node {}", out_e.node.clone()))?;
    //
    //             /// Get all input schemas for the next node
    //             let next_node_input_schemas =
    //                 self.get_node_input_schemas(next_node_handle.clone(), next_node, res);
    //             if next_node_input_schemas.is_none() {
    //                 return Ok(());
    //             }
    //
    //             let r = self.index_node_schemas(
    //                 next_node_handle.clone(),
    //                 next_node,
    //                 next_node_input_schemas,
    //                 dag,
    //                 res,
    //             );
    //
    //             if r.is_err() {
    //                 return r;
    //             }
    //         }
    //     }
    //     Ok(())
    // }
}
