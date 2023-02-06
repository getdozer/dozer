use crate::dag::errors::ExecutionError;
use crate::dag::{Dag, NodeKind};

use crate::dag::node::{NodeHandle, OutputPortType, PortHandle};
use crate::dag::record_store::AutogenRowKeyLookupRecordWriter;
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
pub struct EdgeType<T> {
    pub output_port: PortHandle,
    pub output_port_type: OutputPortType,
    pub input_port: PortHandle,
    pub schema: Schema,
    pub context: T,
}

impl<T> EdgeType<T> {
    pub fn new(
        output_port: PortHandle,
        output_port_type: OutputPortType,
        input_port: PortHandle,
        schema: Schema,
        context: T,
    ) -> Self {
        Self {
            output_port,
            output_port_type,
            input_port,
            schema,
            context,
        }
    }
}

#[derive(Debug, Clone)]
/// `DagSchemas` is a `Dag` with validated schema on the edge.
pub struct DagSchemas<T: Clone> {
    pub graph: daggy::Dag<NodeType<T>, EdgeType<T>>,
}

impl<T: Clone + Debug> DagSchemas<T> {
    /// Validate and populate the schemas, the resultant DAG will have the exact same structure as the input DAG,
    /// with validated schema information on the edges.
    pub fn new(dag: &Dag<T>) -> Result<Self, ExecutionError> {
        let graph = populate_schemas(dag.graph())?;
        Ok(Self { graph })
    }

    pub fn get_node_input_schemas(
        &self,
        node_index: NodeIndex,
    ) -> HashMap<PortHandle, (Schema, T)> {
        let mut schemas = HashMap::new();

        for edge in self.graph.edges_directed(node_index, Direction::Incoming) {
            let edge = edge.weight();
            schemas.insert(edge.input_port, (edge.schema.clone(), edge.context.clone()));
        }

        schemas
    }

    pub fn get_node_output_schemas(
        &self,
        node_index: NodeIndex,
    ) -> HashMap<PortHandle, (Schema, T)> {
        let mut schemas = HashMap::new();

        for edge in self.graph.edges(node_index) {
            let edge = edge.weight();
            schemas.insert(
                edge.output_port,
                (edge.schema.clone(), edge.context.clone()),
            );
        }

        schemas
    }

    pub fn get_all_schemas(&self) -> HashMap<NodeHandle, NodeSchemas<T>> {
        let mut schemas = HashMap::new();

        for (node_index, node) in self.graph.node_references() {
            let node_handle = node.handle.clone();
            let input_schemas = self.get_node_input_schemas(node_index);
            let output_schemas = self.get_node_output_schemas(node_index);
            schemas.insert(
                node_handle,
                NodeSchemas::from(input_schemas, output_schemas),
            );
        }

        schemas
    }

    pub fn prepare(&self) -> Result<(), ExecutionError> {
        for (node_index, node) in self.graph.node_references() {
            match &node.kind {
                NodeKind::Source(source) => {
                    let output_schemas = self.get_node_output_schemas(node_index);
                    source.prepare(output_schemas)?;
                }
                NodeKind::Processor(processor) => {
                    let input_schemas = self.get_node_input_schemas(node_index);
                    let output_schemas = self.get_node_output_schemas(node_index);
                    processor.prepare(input_schemas, output_schemas)?;
                }
                NodeKind::Sink(sink) => {
                    let input_schemas = self.get_node_input_schemas(node_index);
                    sink.prepare(input_schemas)?;
                }
            }
        }
        Ok(())
    }
}

/// In topological order, pass output schemas to downstream nodes' input schemas.
fn populate_schemas<T: Clone + Debug>(
    dag: &daggy::Dag<NodeType<T>, DagEdgeType>,
) -> Result<daggy::Dag<NodeType<T>, EdgeType<T>>, ExecutionError> {
    let mut edges = vec![None; dag.graph().edge_count()];

    for node_index in Topo::new(dag).iter(dag) {
        let node = &dag.graph()[node_index];

        match &node.kind {
            NodeKind::Source(source) => {
                let ports = source.get_output_ports()?;

                for edge in dag.graph().edges(node_index) {
                    let port = find_output_port_def(&ports, edge);
                    let (schema, ctx) = source.get_output_schema(&port.handle)?;
                    let schema = prepare_schema_based_on_output_type(schema, port.typ);
                    create_edge(&mut edges, edge, port, schema, ctx);
                }
            }

            NodeKind::Processor(processor) => {
                let input_schemas =
                    validate_input_schemas(dag, &edges, node_index, processor.get_input_ports())?;

                let ports = processor.get_output_ports();

                for edge in dag.graph().edges(node_index) {
                    let port = find_output_port_def(&ports, edge);
                    let (schema, ctx) =
                        processor.get_output_schema(&port.handle, &input_schemas)?;
                    let schema = prepare_schema_based_on_output_type(schema, port.typ);
                    create_edge(&mut edges, edge, port, schema, ctx);
                }
            }

            NodeKind::Sink(sink) => {
                validate_input_schemas(dag, &edges, node_index, sink.get_input_ports())?;
            }
        }
    }

    Ok(dag.map(
        |_, node| node.clone(),
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

fn prepare_schema_based_on_output_type(schema: Schema, typ: OutputPortType) -> Schema {
    match typ {
        OutputPortType::Stateless | OutputPortType::StatefulWithPrimaryKeyLookup { .. } => schema,
        OutputPortType::AutogenRowKeyLookup => {
            AutogenRowKeyLookupRecordWriter::prepare_schema(schema)
        }
    }
}

fn create_edge<T: Clone>(
    edges: &mut [Option<EdgeType<T>>],
    edge: EdgeReference<DagEdgeType>,
    port: &OutputPortDef,
    schema: Schema,
    ctx: T,
) {
    debug_assert!(port.handle == edge.weight().from);
    let edge_ref = &mut edges[edge.id().index()];
    debug_assert!(edge_ref.is_none());
    *edge_ref = Some(EdgeType::new(
        port.handle,
        port.typ,
        edge.weight().to,
        schema,
        ctx,
    ));
}

fn validate_input_schemas<T: Clone>(
    dag: &daggy::Dag<NodeType<T>, DagEdgeType>,
    edges: &[Option<EdgeType<T>>],
    node_index: NodeIndex,
    input_ports: Vec<PortHandle>,
) -> Result<HashMap<PortHandle, (Schema, T)>, ExecutionError> {
    let node_handle = &dag.graph()[node_index].handle;

    let mut input_schemas = HashMap::new();
    for edge in dag.graph().edges_directed(node_index, Direction::Incoming) {
        let port_handle = edge.weight().to;

        let edge = edges[edge.id().index()].as_ref().expect(
            "This edge has been created from the source node because we traverse in topological order"
        );

        if input_schemas
            .insert(port_handle, (edge.schema.clone(), edge.context.clone()))
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
