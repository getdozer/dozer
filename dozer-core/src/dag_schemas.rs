use crate::errors::ExecutionError;
use crate::{Dag, EdgeHavePorts, NodeKind};

use crate::node::{OutputPortType, PortHandle};
use daggy::petgraph::graph::EdgeReference;
use daggy::petgraph::visit::{EdgeRef, IntoEdges, IntoEdgesDirected, IntoNodeReferences, Topo};
use daggy::petgraph::Direction;
use daggy::{NodeIndex, Walker};
use dozer_types::log::{error, info};
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::fmt::Debug;

use super::node::OutputPortDef;
use super::{EdgeType as DagEdgeType, NodeType};

#[derive(Debug, Clone)]
pub struct NodeSchemas {
    pub input_schemas: HashMap<PortHandle, Schema>,
    pub output_schemas: HashMap<PortHandle, Schema>,
}

impl Default for NodeSchemas {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeSchemas {
    pub fn new() -> Self {
        Self {
            input_schemas: HashMap::new(),
            output_schemas: HashMap::new(),
        }
    }
    pub fn from(
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Self {
        Self {
            input_schemas,
            output_schemas,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
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

pub trait EdgeHaveSchema: EdgeHavePorts {
    fn schema(&self) -> &Schema;
}

impl EdgeHavePorts for EdgeType {
    fn output_port(&self) -> PortHandle {
        self.output_port
    }

    fn input_port(&self) -> PortHandle {
        self.input_port
    }
}

impl EdgeHaveSchema for EdgeType {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug)]
/// `DagSchemas` is a `Dag` with validated schema on the edge.
pub struct DagSchemas {
    graph: daggy::Dag<NodeType, EdgeType>,
}

impl DagSchemas {
    pub fn into_graph(self) -> daggy::Dag<NodeType, EdgeType> {
        self.graph
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType, EdgeType> {
        &self.graph
    }
}

impl DagSchemas {
    /// Validate and populate the schemas, the resultant DAG will have the exact same structure as the input DAG,
    /// with validated schema information on the edges.
    pub fn new(dag: Dag) -> Result<Self, ExecutionError> {
        validate_connectivity(&dag);

        match populate_schemas(dag.into_graph()) {
            Ok(graph) => {
                info!("[pipeline] Validation completed");
                Ok(Self { graph })
            }
            Err(e) => {
                error!("[pipeline] Validation error: {}", e);
                Err(e)
            }
        }
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

impl DagHaveSchemas for DagSchemas {
    type NodeType = NodeType;
    type EdgeType = EdgeType;

    fn graph(&self) -> &daggy::Dag<Self::NodeType, Self::EdgeType> {
        &self.graph
    }
}

fn validate_connectivity(dag: &Dag) {
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
fn populate_schemas(
    dag: daggy::Dag<NodeType, DagEdgeType>,
) -> Result<daggy::Dag<NodeType, EdgeType>, ExecutionError> {
    let mut edges = vec![None; dag.graph().edge_count()];

    for node_index in Topo::new(&dag).iter(&dag) {
        let node = &dag.graph()[node_index];

        match &node.kind {
            NodeKind::Source(source) => {
                let ports = source.get_output_ports();

                for edge in dag.graph().edges(node_index) {
                    let port = find_output_port_def(&ports, edge);
                    let schema = source
                        .get_output_schema(&port.handle)
                        .map_err(ExecutionError::Factory)?;
                    create_edge(&mut edges, edge, port, schema);
                }
            }

            NodeKind::Processor(processor) => {
                let input_schemas =
                    validate_input_schemas(&dag, &edges, node_index, processor.get_input_ports())?;

                let ports = processor.get_output_ports();

                for edge in dag.graph().edges(node_index) {
                    let port = find_output_port_def(&ports, edge);
                    let schema = processor
                        .get_output_schema(&port.handle, &input_schemas)
                        .map_err(ExecutionError::Factory)?;
                    create_edge(&mut edges, edge, port, schema);
                }
            }

            NodeKind::Sink(sink) => {
                let input_schemas =
                    validate_input_schemas(&dag, &edges, node_index, sink.get_input_ports())?;
                sink.prepare(input_schemas)
                    .map_err(ExecutionError::Factory)?;
            }
        }
    }

    Ok(dag.map_owned(
        |_, node| node,
        |edge, _| edges[edge.index()].take().expect("We traversed every edge"),
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

fn create_edge(
    edges: &mut [Option<EdgeType>],
    edge: EdgeReference<DagEdgeType>,
    port: &OutputPortDef,
    schema: Schema,
) {
    debug_assert!(port.handle == edge.weight().from);
    let edge_ref = &mut edges[edge.id().index()];
    debug_assert!(edge_ref.is_none());
    *edge_ref = Some(EdgeType::new(
        port.handle,
        port.typ,
        edge.weight().to,
        schema,
    ));
}

fn validate_input_schemas(
    dag: &daggy::Dag<NodeType, DagEdgeType>,
    edge_and_contexts: &[Option<EdgeType>],
    node_index: NodeIndex,
    input_ports: Vec<PortHandle>,
) -> Result<HashMap<PortHandle, Schema>, ExecutionError> {
    let node_handle = &dag.graph()[node_index].handle;

    let mut input_schemas = HashMap::new();
    for edge in dag.graph().edges_directed(node_index, Direction::Incoming) {
        let port_handle = edge.weight().to;

        let edge = edge_and_contexts[edge.id().index()].as_ref().expect(
            "This edge has been created from the source node because we traverse in topological order"
        );

        if input_schemas
            .insert(port_handle, edge.schema.clone())
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
            Box::new(ConnectivityTestSourceFactory),
        );
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn processor_with_no_outgoing_edge_should_panic() {
        let mut dag = Dag::new();
        let source = dag.add_source(
            NodeHandle::new(None, "source".to_string()),
            Box::new(ConnectivityTestSourceFactory),
        );
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Box::new(ConnectivityTestProcessorFactory),
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
            Box::new(NoInputPortSinkFactory),
        );
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn processor_with_no_input_port_should_panic() {
        let mut dag = Dag::new();
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Box::new(NoInputPortProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Box::new(ConnectivityTestSinkFactory),
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
            Box::new(ConnectivityTestSinkFactory),
        );
        validate_connectivity(&dag);
    }

    #[test]
    #[should_panic]
    fn sink_with_over_connected_input_port_should_panic() {
        let mut dag = Dag::new();
        let source1 = dag.add_source(
            NodeHandle::new(None, "source1".to_string()),
            Box::new(ConnectivityTestSourceFactory),
        );
        let source2 = dag.add_source(
            NodeHandle::new(None, "source2".to_string()),
            Box::new(ConnectivityTestSourceFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Box::new(ConnectivityTestSinkFactory),
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
            Box::new(ConnectivityTestProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Box::new(ConnectivityTestSinkFactory),
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
            Box::new(ConnectivityTestSourceFactory),
        );
        let source2 = dag.add_source(
            NodeHandle::new(None, "source2".to_string()),
            Box::new(ConnectivityTestSourceFactory),
        );
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor".to_string()),
            Box::new(ConnectivityTestProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Box::new(ConnectivityTestSinkFactory),
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
            Box::new(ConnectivityTestSourceFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Box::new(ConnectivityTestSinkFactory),
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
            Box::new(ConnectivityTestSourceFactory),
        );
        let processor = dag.add_processor(
            NodeHandle::new(None, "processor1".to_string()),
            Box::new(ConnectivityTestProcessorFactory),
        );
        let sink = dag.add_sink(
            NodeHandle::new(None, "sink".to_string()),
            Box::new(ConnectivityTestSinkFactory),
        );
        dag.connect_with_index(source, DEFAULT_PORT_HANDLE, processor, DEFAULT_PORT_HANDLE)
            .unwrap();
        dag.connect_with_index(processor, DEFAULT_PORT_HANDLE, sink, DEFAULT_PORT_HANDLE)
            .unwrap();
        validate_connectivity(&dag);
    }
}
