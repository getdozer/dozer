use daggy::petgraph::visit::{Bfs, EdgeRef, IntoEdges};
use daggy::Walker;
use dozer_types::node::NodeHandle;

use crate::errors::ExecutionError;
use crate::node::{PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};

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

#[derive(Debug)]
/// A `SourceFactory`, `ProcessorFactory` or `SinkFactory`.
pub enum NodeKind {
    Source(Box<dyn SourceFactory>),
    Processor(Box<dyn ProcessorFactory>),
    Sink(Box<dyn SinkFactory>),
}

#[derive(Debug)]
/// The node type of the description DAG.
pub struct NodeType {
    /// The node handle, unique across the DAG.
    pub handle: NodeHandle,
    /// The node kind.
    pub kind: NodeKind,
}

impl Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.handle.id)
    }
}

#[derive(Debug, Clone, Copy)]
/// The edge type of the description DAG.
pub struct EdgeType {
    pub from: PortHandle,
    pub to: PortHandle,
}

impl EdgeType {
    pub fn new(from: PortHandle, to: PortHandle) -> Self {
        Self { from, to }
    }
}

pub trait EdgeHavePorts {
    fn output_port(&self) -> PortHandle;
    fn input_port(&self) -> PortHandle;
}

impl EdgeHavePorts for EdgeType {
    fn output_port(&self) -> PortHandle {
        self.from
    }

    fn input_port(&self) -> PortHandle {
        self.to
    }
}

#[derive(Debug)]
pub struct Dag {
    /// The underlying graph.
    graph: daggy::Dag<NodeType, EdgeType>,
    /// Map from node handle to node index.
    node_lookup_table: HashMap<NodeHandle, daggy::NodeIndex>,
    /// All edge indexes.
    edge_indexes: HashSet<EdgeIndex>,
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

impl Dag {
    /// Creates an empty DAG.
    pub fn new() -> Self {
        Self {
            graph: daggy::Dag::new(),
            node_lookup_table: HashMap::new(),
            edge_indexes: HashSet::new(),
        }
    }

    /// Returns the underlying daggy graph.
    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }

    /// Returns the underlying daggy graph.
    pub fn into_graph(self) -> daggy::Dag<NodeType, EdgeType> {
        self.graph
    }

    /// Adds a source. Panics if the `handle` exists in the `Dag`.
    pub fn add_source(
        &mut self,
        handle: NodeHandle,
        source: Box<dyn SourceFactory>,
    ) -> daggy::NodeIndex {
        self.add_node(handle, NodeKind::Source(source))
    }

    /// Adds a processor. Panics if the `handle` exists in the `Dag`.
    pub fn add_processor(
        &mut self,
        handle: NodeHandle,
        processor: Box<dyn ProcessorFactory>,
    ) -> daggy::NodeIndex {
        self.add_node(handle, NodeKind::Processor(processor))
    }

    /// Adds a sink. Panics if the `handle` exists in the `Dag`.
    pub fn add_sink(&mut self, handle: NodeHandle, sink: Box<dyn SinkFactory>) -> daggy::NodeIndex {
        self.add_node(handle, NodeKind::Sink(sink))
    }

    /// Adds an edge. Panics if there's already an edge from `from` to `to`.
    ///
    /// Returns an error if any of the port cannot be found or the edge would create a cycle.
    pub fn connect(&mut self, from: Endpoint, to: Endpoint) -> Result<(), ExecutionError> {
        let from_node_index = validate_endpoint(self, &from, PortDirection::Output)?;
        let to_node_index = validate_endpoint(self, &to, PortDirection::Input)?;
        self.connect_with_index(from_node_index, from.port, to_node_index, to.port)
    }

    /// Adds an edge. Panics if there's already an edge from `from` to `to`.
    ///
    /// Returns an error if any of the port cannot be found or the edge would create a cycle.
    pub fn connect_with_index(
        &mut self,
        from_node_index: daggy::NodeIndex,
        output_port: PortHandle,
        to_node_index: daggy::NodeIndex,
        input_port: PortHandle,
    ) -> Result<(), ExecutionError> {
        validate_port_with_index(self, from_node_index, output_port, PortDirection::Output)?;
        validate_port_with_index(self, to_node_index, input_port, PortDirection::Input)?;
        let edge_index = self.graph.add_edge(
            from_node_index,
            to_node_index,
            EdgeType::new(output_port, input_port),
        )?;

        if !self.edge_indexes.insert(EdgeIndex {
            from_node: from_node_index,
            output_port,
            to_node: to_node_index,
            input_port,
        }) {
            panic!("An edge {edge_index:?} has already been inserted using specified edge handle");
        }

        Ok(())
    }

    /// Adds another whole `Dag` to `self`. Optionally under a namespace `ns`.
    pub fn merge(&mut self, ns: Option<u16>, other: Dag) {
        let (other_nodes, _) = other.graph.into_graph().into_nodes_edges();

        // Insert nodes.
        let mut other_node_index_to_self_node_index = vec![];
        for other_node in other_nodes.into_iter() {
            let other_node = other_node.weight;
            let self_node_handle =
                NodeHandle::new(ns.or(other_node.handle.ns), other_node.handle.id.clone());
            let self_node_index = self.add_node(self_node_handle.clone(), other_node.kind);
            other_node_index_to_self_node_index.push(self_node_index);
        }

        // Insert edges.
        for other_edge_index in other.edge_indexes.into_iter() {
            let self_from_node =
                other_node_index_to_self_node_index[other_edge_index.from_node.index()];
            let self_to_node =
                other_node_index_to_self_node_index[other_edge_index.to_node.index()];
            self.connect_with_index(
                self_from_node,
                other_edge_index.output_port,
                self_to_node,
                other_edge_index.input_port,
            )
            .expect("BUG in DAG");
        }
    }

    /// Returns an iterator over all node handles.
    pub fn node_handles(&self) -> impl Iterator<Item = &NodeHandle> {
        self.nodes().map(|node| &node.handle)
    }

    /// Returns an iterator over all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeType> {
        self.graph.raw_nodes().iter().map(|node| &node.weight)
    }

    /// Returns an iterator over source handles and sources.
    pub fn sources(&self) -> impl Iterator<Item = (&NodeHandle, &dyn SourceFactory)> {
        self.nodes().flat_map(|node| {
            if let NodeKind::Source(source) = &node.kind {
                Some((&node.handle, &**source))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over processor handles and processors.
    pub fn processors(&self) -> impl Iterator<Item = (&NodeHandle, &dyn ProcessorFactory)> {
        self.nodes().flat_map(|node| {
            if let NodeKind::Processor(processor) = &node.kind {
                Some((&node.handle, &**processor))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over sink handles and sinks.
    pub fn sinks(&self) -> impl Iterator<Item = (&NodeHandle, &dyn SinkFactory)> {
        self.nodes().flat_map(|node| {
            if let NodeKind::Sink(sink) = &node.kind {
                Some((&node.handle, &**sink))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over all edge handles.
    pub fn edge_handles(&self) -> Vec<Edge> {
        let get_endpoint = |node_index: daggy::NodeIndex, port_handle| {
            let node = &self.graph[node_index];
            Endpoint {
                node: node.handle.clone(),
                port: port_handle,
            }
        };

        self.edge_indexes
            .iter()
            .map(|edge_index| {
                Edge::new(
                    get_endpoint(edge_index.from_node, edge_index.output_port),
                    get_endpoint(edge_index.to_node, edge_index.input_port),
                )
            })
            .collect()
    }

    /// Finds the node by its handle.
    pub fn node_kind_from_handle(&self, handle: &NodeHandle) -> &NodeKind {
        &self.graph[self.node_index(handle)].kind
    }

    /// Returns an iterator over node handles that are connected to the given node handle.
    pub fn edges_from_handle(&self, handle: &NodeHandle) -> impl Iterator<Item = &NodeHandle> {
        let node_index = self.node_index(handle);
        self.graph
            .edges(node_index)
            .map(|edge| &self.graph[edge.target()].handle)
    }

    /// Returns an iterator over endpoints that are connected to the given endpoint.
    pub fn edges_from_endpoint<'a>(
        &'a self,
        node_handle: &'a NodeHandle,
        port_handle: PortHandle,
    ) -> impl Iterator<Item = (&NodeHandle, PortHandle)> {
        self.graph
            .edges(self.node_index(node_handle))
            .filter(move |edge| edge.weight().from == port_handle)
            .map(|edge| (&self.graph[edge.target()].handle, edge.weight().to))
    }

    /// Returns an iterator over all node handles reachable from `start` in a breadth-first search.
    pub fn bfs(&self, start: &NodeHandle) -> impl Iterator<Item = &NodeHandle> {
        let start = self.node_index(start);

        Bfs::new(self.graph.graph(), start)
            .iter(self.graph.graph())
            .map(|node_index| &self.graph[node_index].handle)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EdgeIndex {
    from_node: daggy::NodeIndex,
    output_port: PortHandle,
    to_node: daggy::NodeIndex,
    input_port: PortHandle,
}

impl Dag {
    fn add_node(&mut self, handle: NodeHandle, kind: NodeKind) -> daggy::NodeIndex {
        let node_index = self.graph.add_node(NodeType {
            handle: handle.clone(),
            kind,
        });
        if let Some(node_index) = self.node_lookup_table.insert(handle, node_index) {
            panic!("A node {node_index:?} has already been inserted using specified node handle");
        }
        node_index
    }

    fn node_index(&self, node_handle: &NodeHandle) -> daggy::NodeIndex {
        *self
            .node_lookup_table
            .get(node_handle)
            .unwrap_or_else(|| panic!("Node handle {node_handle:?} not found in dag"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PortDirection {
    Input,
    Output,
}

fn validate_endpoint(
    dag: &Dag,
    endpoint: &Endpoint,
    direction: PortDirection,
) -> Result<daggy::NodeIndex, ExecutionError> {
    let node_index = dag.node_index(&endpoint.node);
    validate_port_with_index(dag, node_index, endpoint.port, direction)?;
    Ok(node_index)
}

fn validate_port_with_index(
    dag: &Dag,
    node_index: daggy::NodeIndex,
    port: PortHandle,
    direction: PortDirection,
) -> Result<(), ExecutionError> {
    let node = &dag.graph[node_index];
    if !contains_port(&node.kind, direction, port)? {
        return Err(ExecutionError::InvalidPortHandle(port));
    }
    Ok(())
}

fn contains_port(
    node: &NodeKind,
    direction: PortDirection,
    port: PortHandle,
) -> Result<bool, ExecutionError> {
    Ok(match node {
        NodeKind::Processor(p) => {
            if direction == PortDirection::Output {
                p.get_output_ports().iter().any(|e| e == &port)
            } else {
                p.get_input_ports().contains(&port)
            }
        }
        NodeKind::Sink(s) => {
            if direction == PortDirection::Output {
                false
            } else {
                s.get_input_ports().contains(&port)
            }
        }
        NodeKind::Source(s) => {
            if direction == PortDirection::Output {
                s.get_output_ports().iter().any(|e| e.handle == port)
            } else {
                false
            }
        }
    })
}
