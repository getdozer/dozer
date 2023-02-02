use daggy::petgraph::visit::{Bfs, EdgeRef, IntoEdges, Topo};
use daggy::Walker;

use crate::dag::errors::ExecutionError;
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};

use std::collections::{HashMap, HashSet};
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

pub enum NodeType<T> {
    Source(Arc<dyn SourceFactory<T>>),
    Processor(Arc<dyn ProcessorFactory<T>>),
    Sink(Arc<dyn SinkFactory<T>>),
}

struct EdgeType {
    from: PortHandle,
    to: PortHandle,
}

impl EdgeType {
    fn new(from: PortHandle, to: PortHandle) -> Self {
        Self { from, to }
    }
}

pub struct Dag<T> {
    /// The underlying graph.
    graph: daggy::Dag<NodeType<T>, EdgeType>,
    /// Map from node handle to node index.
    node_lookup_table: HashMap<NodeHandle, daggy::NodeIndex>,
    /// Map from node index to node handle.
    node_handles: Vec<NodeHandle>,
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
            node_handles: vec![],
            edge_handles: HashSet::new(),
        }
    }

    /// Adds a node. Panics if the `handle` exists in the `Dag`.
    pub fn add_node(&mut self, node_builder: NodeType<T>, handle: NodeHandle) {
        let node_index = self.graph.add_node(node_builder);
        self.node_handles.push(handle.clone());
        if let Some(node_index) = self.node_lookup_table.insert(handle, node_index) {
            panic!("A node {node_index:?} has already been inserted using specified node handle");
        }
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
        for (other_node, other_node_handle) in
            other_nodes.into_iter().zip(other.node_handles.into_iter())
        {
            let self_node_handle =
                NodeHandle::new(ns.or(other_node_handle.ns), other_node_handle.id.clone());
            self.add_node(other_node.weight, self_node_handle.clone());
            other_node_handle_to_self_node_handle.insert(other_node_handle, self_node_handle);
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
    pub fn node_handles(&self) -> &[NodeHandle] {
        &self.node_handles
    }

    /// Returns an iterator over all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = (&NodeHandle, &NodeType<T>)> {
        self.node_lookup_table
            .iter()
            .map(|(node_handle, node_index)| {
                (
                    node_handle,
                    self.graph.node_weight(*node_index).expect("BUG in DAG"),
                )
            })
    }

    /// Returns an iterator over source handles and sources.
    pub fn sources(&self) -> impl Iterator<Item = (&NodeHandle, &Arc<dyn SourceFactory<T>>)> {
        self.nodes().flat_map(|(node_handle, node)| {
            if let NodeType::Source(source) = node {
                Some((node_handle, source))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over processor handles and processors.
    pub fn processors(&self) -> impl Iterator<Item = (&NodeHandle, &Arc<dyn ProcessorFactory<T>>)> {
        self.nodes().flat_map(|(node_handle, node)| {
            if let NodeType::Processor(processor) = node {
                Some((node_handle, processor))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over sink handles and sinks.
    pub fn sinks(&self) -> impl Iterator<Item = (&NodeHandle, &Arc<dyn SinkFactory<T>>)> {
        self.nodes().flat_map(|(node_handle, node)| {
            if let NodeType::Sink(sink) = node {
                Some((node_handle, sink))
            } else {
                None
            }
        })
    }

    /// Returns an iterator over all edge handles.
    pub fn edges(&self) -> impl Iterator<Item = &Edge> {
        self.edge_handles.iter()
    }

    /// Finds the node by its handle.
    pub fn node_from_handle(&self, handle: &NodeHandle) -> &NodeType<T> {
        let node_index = self.node_index(handle);
        self.graph.node_weight(node_index).expect("BUG in DAG")
    }

    /// Returns an iterator over node handles that are connected to the given node handle.
    pub fn edges_from_handle(&self, handle: &NodeHandle) -> impl Iterator<Item = &NodeHandle> {
        let node_index = self.node_index(handle);
        self.graph
            .edges(node_index)
            .map(|edge| &self.node_handles[edge.target().index()])
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
            .map(|edge| (&self.node_handles[edge.target().index()], edge.weight().to))
    }

    /// Returns an iterator over all node handles reachable from `start` in a breadth-first search.
    pub fn bfs(&self, start: &NodeHandle) -> impl Iterator<Item = &NodeHandle> {
        let start = self.node_index(start);

        Bfs::new(self.graph.graph(), start)
            .iter(self.graph.graph())
            .map(|node_index| &self.node_handles[node_index.index()])
    }

    /// Returns an iterator over all node handles in topological order.
    pub fn topo(&self) -> impl Iterator<Item = &NodeHandle> {
        Topo::new(self.graph.graph())
            .iter(self.graph.graph())
            .map(|node_index| &self.node_handles[node_index.index()])
    }
}

impl<T> Dag<T> {
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
    let node = dag.graph.node_weight(node_index).expect("BUG in DAG");
    if !contains_port(node, direction, endpoint.port)? {
        return Err(ExecutionError::InvalidPortHandle(endpoint.port));
    }
    Ok(node_index)
}

fn contains_port<T>(
    node: &NodeType<T>,
    direction: PortDirection,
    port: PortHandle,
) -> Result<bool, ExecutionError> {
    Ok(match node {
        NodeType::Processor(p) => {
            if direction == PortDirection::Output {
                p.get_output_ports().iter().any(|e| e.handle == port)
            } else {
                p.get_input_ports().contains(&port)
            }
        }
        NodeType::Sink(s) => {
            if direction == PortDirection::Output {
                false
            } else {
                s.get_input_ports().contains(&port)
            }
        }
        NodeType::Source(s) => {
            if direction == PortDirection::Output {
                s.get_output_ports()?.iter().any(|e| e.handle == port)
            } else {
                false
            }
        }
    })
}
