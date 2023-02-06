use daggy::petgraph::dot;
use daggy::petgraph::visit::{Bfs, EdgeRef, IntoEdges, IntoNodeReferences};
use daggy::Walker;

use crate::dag::errors::ExecutionError;
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
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

#[derive(Debug, Clone)]
pub enum NodeKind<T> {
    Source(Arc<dyn SourceFactory<T>>),
    Processor(Arc<dyn ProcessorFactory<T>>),
    Sink(Arc<dyn SinkFactory<T>>),
}

#[derive(Debug, Clone)]
pub struct NodeType<T> {
    pub handle: NodeHandle,
    pub kind: NodeKind<T>,
}

impl<T> Display for NodeType<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.handle)
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
impl Display for EdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} -> {:?}", self.from, self.to)
    }
}

pub struct Dag<T> {
    /// The underlying graph.
    graph: daggy::Dag<NodeType<T>, EdgeType>,
    /// Map from node handle to node index.
    node_lookup_table: HashMap<NodeHandle, daggy::NodeIndex>,
    /// All edge handles.
    edge_handles: HashSet<Edge>,
}

impl<T> Default for Dag<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Dag<T> {
    /// Creates an empty DAG.
    pub fn new() -> Self {
        Self {
            graph: daggy::Dag::new(),
            node_lookup_table: HashMap::new(),
            edge_handles: HashSet::new(),
        }
    }

    /// Returns the underlying daggy graph.
    pub fn graph(&self) -> &daggy::Dag<NodeType<T>, EdgeType> {
        &self.graph
    }

    pub fn print_dot(&self) {
        use std::println as info;
        info!("{}", dot::Dot::new(&self.graph));
    }

    /// Adds a source. Panics if the `handle` exists in the `Dag`.
    pub fn add_source(
        &mut self,
        handle: NodeHandle,
        source: Arc<dyn SourceFactory<T>>,
    ) -> daggy::NodeIndex {
        self.add_node(handle, NodeKind::Source(source))
    }

    /// Adds a processor. Panics if the `handle` exists in the `Dag`.
    pub fn add_processor(
        &mut self,
        handle: NodeHandle,
        processor: Arc<dyn ProcessorFactory<T>>,
    ) -> daggy::NodeIndex {
        self.add_node(handle, NodeKind::Processor(processor))
    }

    /// Adds a sink. Panics if the `handle` exists in the `Dag`.
    pub fn add_sink(
        &mut self,
        handle: NodeHandle,
        sink: Arc<dyn SinkFactory<T>>,
    ) -> daggy::NodeIndex {
        self.add_node(handle, NodeKind::Sink(sink))
    }

    /// Adds an edge. Panics if there's already an edge from `from` to `to`.
    ///
    /// Returns an error if any of the port cannot be found or the edge would create a cycle.
    pub fn connect(&mut self, from: Endpoint, to: Endpoint) -> Result<(), ExecutionError> {
        let from_node_index = validate_endpoint(self, &from, PortDirection::Output)?;
        let to_node_index = validate_endpoint(self, &to, PortDirection::Input)?;
        let edge_index = self.graph.add_edge(
            from_node_index,
            to_node_index,
            EdgeType::new(from.port, to.port),
        )?;

        if !self.edge_handles.insert(Edge::new(from, to)) {
            panic!("An edge {edge_index:?} has already been inserted using specified edge handle");
        }

        Ok(())
    }

    /// Adds another whole `Dag` to `self`. Optionally under a namespace `ns`.
    pub fn merge(&mut self, ns: Option<u16>, other: Dag<T>) {
        let (other_nodes, _) = other.graph.into_graph().into_nodes_edges();

        // Insert nodes.
        let mut other_node_handle_to_self_node_handle = HashMap::new();
        for other_node in other_nodes.into_iter() {
            let other_node = other_node.weight;
            let self_node_handle =
                NodeHandle::new(ns.or(other_node.handle.ns), other_node.handle.id.clone());
            self.add_node(self_node_handle.clone(), other_node.kind);
            other_node_handle_to_self_node_handle.insert(other_node.handle, self_node_handle);
        }

        // Insert edges.
        let map_endpoint = |other_endpoint: Endpoint| {
            let self_node_handle = other_node_handle_to_self_node_handle
                .get(&other_endpoint.node)
                .expect("BUG in DAG");
            Endpoint::new(self_node_handle.clone(), other_endpoint.port)
        };
        for other_edge_handle in other.edge_handles.into_iter() {
            let from = map_endpoint(other_edge_handle.from);
            let to = map_endpoint(other_edge_handle.to);
            self.connect(from, to).expect("BUG in DAG");
        }
    }

    /// Returns an iterator over all node handles.
    pub fn node_handles(&self) -> impl Iterator<Item = &NodeHandle> {
        self.nodes().map(|node| &node.handle)
    }

    /// Returns an iterator over all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeType<T>> {
        self.graph.raw_nodes().iter().map(|node| &node.weight)
    }

    /// Returns an iterator over source handles and sources.
    pub fn sources(&self) -> impl Iterator<Item = (&NodeHandle, &Arc<dyn SourceFactory<T>>)> {
        self.nodes().flat_map(|node| {
            if let NodeKind::Source(source) = &node.kind {
                Some((&node.handle, source))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over processor handles and processors.
    pub fn processors(&self) -> impl Iterator<Item = (&NodeHandle, &Arc<dyn ProcessorFactory<T>>)> {
        self.nodes().flat_map(|node| {
            if let NodeKind::Processor(processor) = &node.kind {
                Some((&node.handle, processor))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over sink handles and sinks.
    pub fn sinks(&self) -> impl Iterator<Item = (&NodeHandle, &Arc<dyn SinkFactory<T>>)> {
        self.nodes().flat_map(|node| {
            if let NodeKind::Sink(sink) = &node.kind {
                Some((&node.handle, sink))
            } else {
                None
            }
        })
    }

    pub fn sink_identifiers(&self) -> impl Iterator<Item = daggy::NodeIndex> + '_ {
        self.graph.node_references().flat_map(|(node_index, node)| {
            if let NodeKind::Sink(_) = node.kind {
                Some(node_index)
            } else {
                None
            }
        })
    }

    /// Returns an iterator over all edge handles.
    pub fn edge_handles(&self) -> impl Iterator<Item = &Edge> {
        self.edge_handles.iter()
    }

    /// Finds the node by its handle.
    pub fn node_kind_from_handle(&self, handle: &NodeHandle) -> &NodeKind<T> {
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

impl<T> Dag<T> {
    fn add_node(&mut self, handle: NodeHandle, kind: NodeKind<T>) -> daggy::NodeIndex {
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

fn validate_endpoint<T>(
    dag: &Dag<T>,
    endpoint: &Endpoint,
    direction: PortDirection,
) -> Result<daggy::NodeIndex, ExecutionError> {
    let node_index = dag.node_index(&endpoint.node);
    let node = &dag.graph[node_index];
    if !contains_port(&node.kind, direction, endpoint.port)? {
        return Err(ExecutionError::InvalidPortHandle(endpoint.port));
    }
    Ok(node_index)
}

fn contains_port<T>(
    node: &NodeKind<T>,
    direction: PortDirection,
    port: PortHandle,
) -> Result<bool, ExecutionError> {
    Ok(match node {
        NodeKind::Processor(p) => {
            if direction == PortDirection::Output {
                p.get_output_ports().iter().any(|e| e.handle == port)
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
                s.get_output_ports()?.iter().any(|e| e.handle == port)
            } else {
                false
            }
        }
    })
}
