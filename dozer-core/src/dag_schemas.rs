use crate::errors::ExecutionError;
use crate::{Dag, NodeKind, DEFAULT_PORT_HANDLE};

use crate::node::{OutputPortType, PortHandle};
use daggy::petgraph::graph::EdgeReference;
use daggy::petgraph::visit::{EdgeRef, IntoEdges, IntoEdgesDirected, IntoNodeReferences, Topo};
use daggy::petgraph::Direction;
use daggy::{NodeIndex, Walker};
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::fmt::Debug;

use super::node::OutputPortDef;
use super::{EdgeType as DagEdgeType, NodeType};

#[derive(Debug, Clone)]
pub struct NodeSchemas<T> {
    pub input_schemas: HashMap<PortHandle, (Schema, T)>,
    pub output_schemas: HashMap<PortHandle, (Schema, T)>,
}

impl<T> Default for NodeSchemas<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> NodeSchemas<T> {
    pub fn new() -> Self {
        Self {
            input_schemas: HashMap::new(),
            output_schemas: HashMap::new(),
        }
    }
    pub fn from(
        input_schemas: HashMap<PortHandle, (Schema, T)>,
        output_schemas: HashMap<PortHandle, (Schema, T)>,
    ) -> Self {
        Self {
            input_schemas,
            output_schemas,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EdgeType {
    pub output_port: PortHandle,
    pub output_port_type: OutputPortType,
    pub input_port: PortHandle,
    pub schema: Schema,
}

impl EdgeType {
    pub fn new(
        output_port: PortHandle,
        output_port_type: OutputPortType,
        input_port: PortHandle,
        schema: Schema,
    ) -> Self {
        Self {
            output_port,
            output_port_type,
            input_port,
            schema,
        }
    }
}

pub trait EdgeHaveSchema {
    fn output_port(&self) -> PortHandle;
    fn input_port(&self) -> PortHandle;
    fn schema(&self) -> &Schema;
}

impl EdgeHaveSchema for EdgeType {
    fn output_port(&self) -> PortHandle {
        self.output_port
    }

    fn input_port(&self) -> PortHandle {
        self.input_port
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug, Clone)]
/// `DagSchemas` is a `Dag` with validated schema on the edge.
pub struct DagSchemas<T> {
    graph: daggy::Dag<NodeType<T>, EdgeType>,
}

impl<T> DagSchemas<T> {
    pub fn into_graph(self) -> daggy::Dag<NodeType<T>, EdgeType> {
        self.graph
    }

    pub fn get_sink_schemas(&self) -> HashMap<String, Schema> {
        let mut schemas = HashMap::new();

        for (node_index, node) in self.graph.node_references() {
            if let NodeKind::Sink(_) = &node.kind {
                let mut input_schemas = self.get_node_input_schemas(node_index);

                schemas.insert(
                    node.handle.id.clone(),
                    input_schemas
                        .remove(&DEFAULT_PORT_HANDLE)
                        .expect("Sink must have input schema on default port"),
                );
            }
        }

        schemas
    }
}

impl<T: Clone> DagSchemas<T> {
    /// Validate and populate the schemas, the resultant DAG will have the exact same structure as the input DAG,
    /// with validated schema information on the edges.
    pub fn new(dag: Dag<T>) -> Result<Self, ExecutionError> {
        validate_connectivity(&dag);
        let graph = populate_schemas(dag.into_graph())?;
        Ok(Self { graph })
    }
}

pub trait DagHaveSchemas {
    type NodeType;
    type EdgeType: EdgeHaveSchema;

    fn graph(&self) -> &daggy::Dag<Self::NodeType, Self::EdgeType>;

    fn get_node_input_schemas(&self, node_index: NodeIndex) -> HashMap<PortHandle, Schema> {
        let mut schemas = HashMap::new();

        for edge in self.graph().edges_directed(node_index, Direction::Incoming) {
            let edge = edge.weight();
            let schema = edge.schema();
            schemas.insert(edge.input_port(), schema.clone());
        }

        schemas
    }

    fn get_node_output_schemas(&self, node_index: NodeIndex) -> HashMap<PortHandle, Schema> {
        let mut schemas = HashMap::new();

        for edge in self.graph().edges(node_index) {
            let edge = edge.weight();
            let schema = edge.schema();
            schemas.insert(edge.output_port(), schema.clone());
        }

        schemas
    }
}

impl<T> DagHaveSchemas for DagSchemas<T> {
    type NodeType = NodeType<T>;
    type EdgeType = EdgeType;

    fn graph(&self) -> &daggy::Dag<Self::NodeType, Self::EdgeType> {
        &self.graph
    }
}

fn validate_connectivity<T>(dag: &Dag<T>) {
    // Every source or processor has at least one outgoing edge.
    for (node_index, node) in dag.graph().node_references() {
        match &node.kind {
            NodeKind::Source(_) | NodeKind::Processor(_) => {
                if dag.graph().edges(node_index).count() == 0 {
                    panic!("Node {} has no outgoing edge", node.handle);
                }
            }
            NodeKind::Sink(_) => {}
        }
    }

    // Processor and sink has at least one input port. Every input port has exactly one incoming edge.
    for (node_index, node) in dag.graph().node_references() {
        let mut input_ports = match &node.kind {
            NodeKind::Source(_) => continue,
            NodeKind::Processor(processor) => processor.get_input_ports(),
            NodeKind::Sink(sink) => sink.get_input_ports(),
        };
        if input_ports.is_empty() {
            panic!("Node {} has no input port", node.handle);
        }

        input_ports.sort();

        let mut connected_input_ports = dag
            .graph()
            .edges_directed(node_index, Direction::Incoming)
            .map(|edge| edge.weight().to)
            .collect::<Vec<_>>();
        connected_input_ports.sort();

        if input_ports != connected_input_ports {
            panic!(
                "Node {} has input ports {input_ports:?}, but the incoming edges are {connected_input_ports:?}",
                node.handle
            );
        }
    }
}

/// In topological order, pass output schemas to downstream nodes' input schemas.
fn populate_schemas<T: Clone>(
    dag: daggy::Dag<NodeType<T>, DagEdgeType>,
) -> Result<daggy::Dag<NodeType<T>, EdgeType>, ExecutionError> {
    let mut edges = vec![None; dag.graph().edge_count()];

    for node_index in Topo::new(&dag).iter(&dag) {
        let node = &dag.graph()[node_index];

        match &node.kind {
            NodeKind::Source(source) => {
                let ports = source.get_output_ports();

                for edge in dag.graph().edges(node_index) {
                    let port = find_output_port_def(&ports, edge);
                    let (schema, ctx) = source.get_output_schema(&port.handle)?;
                    create_edge(&mut edges, edge, port, schema, ctx);
                }
            }

            NodeKind::Processor(processor) => {
                let input_schemas =
                    validate_input_schemas(&dag, &edges, node_index, processor.get_input_ports())?;

                let ports = processor.get_output_ports();

                for edge in dag.graph().edges(node_index) {
                    let port = find_output_port_def(&ports, edge);
                    let (schema, ctx) =
                        processor.get_output_schema(&port.handle, &input_schemas)?;
                    create_edge(&mut edges, edge, port, schema, ctx);
                }
            }

            NodeKind::Sink(sink) => {
                let input_schemas =
                    validate_input_schemas(&dag, &edges, node_index, sink.get_input_ports())?;
                sink.prepare(input_schemas)?;
            }
        }
    }

    Ok(dag.map_owned(
        |_, node| node,
        |edge, _| {
            edges[edge.index()]
                .take()
                .expect("We traversed every edge")
                .0
        },
    ))
}

fn find_output_port_def<'a>(
    ports: &'a [OutputPortDef],
    edge: EdgeReference<DagEdgeType>,
) -> &'a OutputPortDef {
    let handle = edge.weight().from;
    for port in ports {
        if port.handle == handle {
            return port;
        }
    }
    panic!("BUG: port {handle} not found")
}

fn create_edge<T>(
    edges: &mut [Option<(EdgeType, T)>],
    edge: EdgeReference<DagEdgeType>,
    port: &OutputPortDef,
    schema: Schema,
    ctx: T,
) {
    debug_assert!(port.handle == edge.weight().from);
    let edge_ref = &mut edges[edge.id().index()];
    debug_assert!(edge_ref.is_none());
    *edge_ref = Some((
        EdgeType::new(port.handle, port.typ, edge.weight().to, schema),
        ctx,
    ));
}

fn validate_input_schemas<T: Clone>(
    dag: &daggy::Dag<NodeType<T>, DagEdgeType>,
    edge_and_contexts: &[Option<(EdgeType, T)>],
    node_index: NodeIndex,
    input_ports: Vec<PortHandle>,
) -> Result<HashMap<PortHandle, (Schema, T)>, ExecutionError> {
    let node_handle = &dag.graph()[node_index].handle;

    let mut input_schemas = HashMap::new();
    for edge in dag.graph().edges_directed(node_index, Direction::Incoming) {
        let port_handle = edge.weight().to;

        let (edge, context) = edge_and_contexts[edge.id().index()].as_ref().expect(
            "This edge has been created from the source node because we traverse in topological order"
        );

        if input_schemas
            .insert(port_handle, (edge.schema.clone(), context.clone()))
            .is_some()
        {
            return Err(ExecutionError::DuplicateInput {
                node: node_handle.clone(),
                port: port_handle,
            });
        }
    }

    for port in input_ports {
        if !input_schemas.contains_key(&port) {
            return Err(ExecutionError::MissingInput {
                node: node_handle.clone(),
                port,
            });
        }
    }
    Ok(input_schemas)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dozer_types::node::NodeHandle;

    use super::*;

    use crate::{
        tests::{
            processors::{ConnectivityTestProcessorFactory, NoInputPortProcessorFactory},
            sinks::{ConnectivityTestSinkFactory, NoInputPortSinkFactory},
            sources::ConnectivityTestSourceFactory,
        },
        DEFAULT_PORT_HANDLE,
    };

    #[test]
    #[should_panic]
    fn source_with_no_outgoing_edge_should_panic() {
        let mut dag = Dag::new();
        dag.add_source(
            NodeHandle::new(None, "source".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn processor_with_no_outgoing_edge_should_panic() {
        let mut dag = Dag::new();
        let source = dag.add_source(
            NodeHandle::new(None, "source".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Arc::new(ConnectivityTestProcessorFactory),
        );
        dag.connect_with_index(source, DEFAULT_PORT_HANDLE, processor, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn sink_with_no_input_port_should_panic() {
        let mut dag = Dag::new();
        dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(NoInputPortSinkFactory),
        );
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn processor_with_no_input_port_should_panic() {
        let mut dag = Dag::new();
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Arc::new(NoInputPortProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(processor, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn sink_with_unconnected_input_port_should_panic() {
        let mut dag = Dag::new();
        dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn sink_with_over_connected_input_port_should_panic() {
        let mut dag = Dag::new();
        let source1 = dag.add_source(
            NodeHandle::new(None, "source1".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let source2 = dag.add_source(
            NodeHandle::new(None, "source2".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(source1, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        dag.connect_with_index(source2, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn processor_with_unconnected_input_port_should_panic() {
        let mut dag = Dag::new();
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Arc::new(ConnectivityTestProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(processor, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn processor_with_over_connected_input_port_should_panic() {
        let mut dag = Dag::new();
        let source1 = dag.add_source(
            NodeHandle::new(None, "source1".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let source2 = dag.add_source(
            NodeHandle::new(None, "source2".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Arc::new(ConnectivityTestProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(source1, DEFAULT_PORT_HANDLE, processor, DEFAULT_PORT_HANDLE)
            .unwrap();
        dag.connect_with_index(source2, DEFAULT_PORT_HANDLE, processor, DEFAULT_PORT_HANDLE)
            .unwrap();
        dag.connect_with_index(processor, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }

    #[test]
    fn validate_source_sink_dag() {
        let mut dag = Dag::new();
        let source = dag.add_source(
            NodeHandle::new(None, "source".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(source, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }

    #[test]
    fn validate_link_shaped_dag() {
        let mut dag = Dag::new();
        let source = dag.add_source(
            NodeHandle::new(None, "source".to_string()),
            Arc::new(ConnectivityTestSourceFactory),
        );
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor1".to_string()),
            Arc::new(ConnectivityTestProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Arc::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(source, DEFAULT_PORT_HANDLE, processor, DEFAULT_PORT_HANDLE)
            .unwrap();
        dag.connect_with_index(processor, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }
}
