use crate::dag::dag::{Dag, NodeKind};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::InvalidNodeHandle;

use crate::dag::node::{NodeHandle, OutputPortType, PortHandle};
use crate::dag::record_store::AutogenRowKeyLookupRecordWriter;
use dozer_types::types::Schema;
use std::collections::HashMap;

#[derive(Clone)]
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

pub struct DagSchemas<T: Clone> {
    schemas: HashMap<NodeHandle, NodeSchemas<T>>,
}

impl<T: Clone> DagSchemas<T> {
    pub fn new(dag: &Dag<T>) -> Result<Self, ExecutionError> {
        let schemas = populate_schemas(dag)?;
        Ok(Self { schemas })
    }

    pub fn get_node_input_schemas(
        &self,
        handle: &NodeHandle,
    ) -> Result<&HashMap<PortHandle, (Schema, T)>, ExecutionError> {
        let node = self
            .schemas
            .get(handle)
            .ok_or_else(|| InvalidNodeHandle(handle.clone()))?;
        Ok(&node.input_schemas)
    }

    pub fn get_node_output_schemas(
        &self,
        handle: &NodeHandle,
    ) -> Result<&HashMap<PortHandle, (Schema, T)>, ExecutionError> {
        let node = self
            .schemas
            .get(handle)
            .ok_or_else(|| InvalidNodeHandle(handle.clone()))?;
        Ok(&node.output_schemas)
    }

    pub fn get_all_schemas(&self) -> &HashMap<NodeHandle, NodeSchemas<T>> {
        &self.schemas
    }
}

pub fn prepare_dag<T: Clone>(
    dag: &Dag<T>,
    dag_schemas: &DagSchemas<T>,
) -> Result<(), ExecutionError> {
    for node in dag.nodes() {
        let schemas = dag_schemas
            .get_all_schemas()
            .get(&node.handle)
            .ok_or_else(|| ExecutionError::InvalidNodeHandle(node.handle.clone()))?;

        match &node.kind {
            NodeKind::Source(s) => s.prepare(schemas.output_schemas.clone())?,
            NodeKind::Sink(s) => s.prepare(schemas.input_schemas.clone())?,
            NodeKind::Processor(p) => p.prepare(
                schemas.input_schemas.clone(),
                schemas.output_schemas.clone(),
            )?,
        }
    }
    Ok(())
}

/// In topological order, pass output schemas to downstream nodes' input schemas.
fn populate_schemas<T: Clone>(
    dag: &Dag<T>,
) -> Result<HashMap<NodeHandle, NodeSchemas<T>>, ExecutionError> {
    let mut dag_schemas = dag
        .node_handles()
        .map(|node_handle| (node_handle.clone(), NodeSchemas::<T>::new()))
        .collect::<HashMap<_, _>>();

    for node_handle in dag.topo() {
        match dag.node_kind_from_handle(node_handle) {
            NodeKind::Source(source) => {
                let ports = source.get_output_ports()?;
                for port in ports {
                    let (schema, ctx) = source.get_output_schema(&port.handle)?;
                    let schema = prepare_schema_based_on_output_type(schema, port.typ);
                    populate_output_schema(
                        dag,
                        &mut dag_schemas,
                        node_handle,
                        port.handle,
                        (schema, ctx),
                    );
                }
            }

            NodeKind::Processor(processor) => {
                let input_schemas =
                    validate_input_schemas(&dag_schemas, node_handle, processor.get_input_ports())?;

                let ports = processor.get_output_ports();
                for port in ports {
                    let (schema, ctx) =
                        processor.get_output_schema(&port.handle, &input_schemas)?;
                    let schema = prepare_schema_based_on_output_type(schema, port.typ);
                    populate_output_schema(
                        dag,
                        &mut dag_schemas,
                        node_handle,
                        port.handle,
                        (schema, ctx),
                    );
                }
            }

            NodeKind::Sink(sink) => {
                validate_input_schemas(&dag_schemas, node_handle, sink.get_input_ports())?;
            }
        }
    }

    Ok(dag_schemas)
}

fn prepare_schema_based_on_output_type(schema: Schema, typ: OutputPortType) -> Schema {
    match typ {
        OutputPortType::Stateless | OutputPortType::StatefulWithPrimaryKeyLookup { .. } => schema,
        OutputPortType::AutogenRowKeyLookup => {
            AutogenRowKeyLookupRecordWriter::prepare_schema(schema)
        }
    }
}

fn populate_output_schema<T: Clone>(
    dag: &Dag<T>,
    dag_schemas: &mut HashMap<NodeHandle, NodeSchemas<T>>,
    node_handle: &NodeHandle,
    port: PortHandle,
    schema: (Schema, T),
) {
    dag_schemas
        .get_mut(node_handle)
        .expect("BUG")
        .output_schemas
        .insert(port, schema.clone());

    for (next_node_handle, next_node_port) in dag.edges_from_endpoint(node_handle, port) {
        let next_node_schemas = dag_schemas.get_mut(next_node_handle).expect("BUG");
        next_node_schemas
            .input_schemas
            .insert(next_node_port, schema.clone());
    }
}

fn validate_input_schemas<T: Clone>(
    dag_schemas: &HashMap<NodeHandle, NodeSchemas<T>>,
    node_handle: &NodeHandle,
    input_ports: Vec<PortHandle>,
) -> Result<HashMap<PortHandle, (Schema, T)>, ExecutionError> {
    let input_schemas = &dag_schemas.get(node_handle).expect("BUG").input_schemas;
    if !eq_ignore_order(input_schemas.keys().copied(), input_ports) {
        return Err(ExecutionError::MissingInput {
            node: node_handle.clone(),
        });
    }
    Ok(input_schemas.clone())
}

fn eq_ignore_order<I: PartialEq + Ord>(a: impl Iterator<Item = I>, mut b: Vec<I>) -> bool {
    let mut a = a.collect::<Vec<_>>();
    a.sort();
    b.sort();
    a == b
}
