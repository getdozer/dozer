use crate::dag::dag::{Dag, NodeType};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::InvalidNodeHandle;

use crate::dag::node::{NodeHandle, OutputPortDef, OutputPortType, PortHandle};
use crate::dag::record_store::AutogenRowKeyLookupRecordWriter;
use dozer_types::types::Schema;
use std::collections::{HashMap, HashSet};

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

pub struct DagSchemaManager<'a, T: Clone> {
    dag: &'a Dag<T>,
    schemas: HashMap<NodeHandle, NodeSchemas<T>>,
}

impl<'a, T: Clone + 'a> DagSchemaManager<'a, T> {
    fn get_port_output_schema(
        node: &NodeType<T>,
        output_port: &OutputPortDef,
        input_schemas: &HashMap<PortHandle, (Schema, T)>,
    ) -> Result<Option<(Schema, T)>, ExecutionError> {
        Ok(match node {
            NodeType::Source(src) => match output_port.typ {
                OutputPortType::Stateless | OutputPortType::StatefulWithPrimaryKeyLookup { .. } => {
                    Some(src.get_output_schema(&output_port.handle)?)
                }
                OutputPortType::AutogenRowKeyLookup => {
                    let (schema, ctx) = src.get_output_schema(&output_port.handle)?;
                    Some((AutogenRowKeyLookupRecordWriter::prepare_schema(schema), ctx))
                }
            },
            NodeType::Processor(proc) => match output_port.typ {
                OutputPortType::Stateless | OutputPortType::StatefulWithPrimaryKeyLookup { .. } => {
                    Some(proc.get_output_schema(&output_port.handle, input_schemas)?)
                }
                OutputPortType::AutogenRowKeyLookup => {
                    let (schema, ctx) =
                        proc.get_output_schema(&output_port.handle, input_schemas)?;
                    Some((AutogenRowKeyLookupRecordWriter::prepare_schema(schema), ctx))
                }
            },
            NodeType::Sink(proc) => {
                proc.set_input_schema(input_schemas)?;
                None
            }
        })
    }

    fn get_node_input_ports(node: &NodeType<T>) -> Vec<PortHandle> {
        match node {
            NodeType::Source(_src) => vec![],
            NodeType::Processor(proc) => proc.get_input_ports(),
            NodeType::Sink(proc) => proc.get_input_ports(),
        }
    }

    fn get_node_output_ports(node: &NodeType<T>) -> Result<Vec<OutputPortDef>, ExecutionError> {
        match node {
            NodeType::Source(src) => src.get_output_ports(),
            NodeType::Processor(proc) => Ok(proc.get_output_ports()),
            NodeType::Sink(_proc) => Ok(vec![]),
        }
    }

    fn fill_node_output_schemas(
        dag: &Dag<T>,
        handle: &NodeHandle,
        all_schemas: &mut HashMap<NodeHandle, NodeSchemas<T>>,
    ) -> Result<(), ExecutionError> {
        // Get the current node
        let node = dag.node_from_handle(handle);

        // Get all input schemas available for this node
        let node_input_schemas_available = HashSet::<&PortHandle>::from_iter(
            all_schemas
                .get(handle)
                .expect("BUG in DagSchemaManager")
                .input_schemas
                .iter()
                .map(|e| e.0),
        );

        // get all input schemas required for this node
        let input_ports = Self::get_node_input_ports(node);
        let node_input_schemas_required = HashSet::<&PortHandle>::from_iter(&input_ports);

        // If we have all input schemas required
        if node_input_schemas_available == node_input_schemas_required {
            match node {
                NodeType::Sink(s) => {
                    let node_schemas = all_schemas
                        .get_mut(handle)
                        .expect("BUG in DagSchemaManager");
                    let _ = s.set_input_schema(&node_schemas.input_schemas);
                }
                _ => {
                    // Calculate the output schema for each port and insert it in the global schemas map
                    for port in &Self::get_node_output_ports(node)? {
                        let schema = {
                            let node_schemas = all_schemas
                                .get_mut(handle)
                                .expect("BUG in DagSchemaManager");
                            let schema = Self::get_port_output_schema(
                                node,
                                port,
                                &node_schemas.input_schemas,
                            )?;
                            if let Some(schema) = &schema {
                                node_schemas
                                    .output_schemas
                                    .insert(port.handle, schema.clone());
                            }
                            schema
                        };

                        // Retrieve all next nodes connected to this port
                        for (next_node_handle, next_node_port) in
                            dag.edges_from_endpoint(handle, port.handle)
                        {
                            let next_node_schemas = all_schemas
                                .get_mut(next_node_handle)
                                .expect("BUG in DagSchemaManager");
                            if let Some(schema) = &schema {
                                next_node_schemas
                                    .input_schemas
                                    .insert(next_node_port, schema.clone());
                            }
                        }
                    }
                }
            }

            for next_node in dag.edges_from_handle(handle) {
                Self::fill_node_output_schemas(dag, next_node, all_schemas)?;
            }
        }

        Ok(())
    }

    pub fn new(dag: &'a Dag<T>) -> Result<Self, ExecutionError> {
        let sources = dag.sources().map(|(handle, _)| handle).collect::<Vec<_>>();

        let mut schemas: HashMap<NodeHandle, NodeSchemas<T>> = dag
            .node_handles()
            .iter()
            .map(|handle| (handle.clone(), NodeSchemas::new()))
            .collect();

        for source in sources {
            Self::fill_node_output_schemas(dag, source, &mut schemas)?;
        }

        Ok(Self { dag, schemas })
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

    pub fn prepare(&self) -> Result<(), ExecutionError> {
        for (handle, node) in self.dag.nodes() {
            let schemas = self
                .schemas
                .get(handle)
                .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

            match node {
                NodeType::Source(s) => s.prepare(schemas.output_schemas.clone())?,
                NodeType::Sink(s) => s.prepare(schemas.input_schemas.clone())?,
                NodeType::Processor(p) => p.prepare(
                    schemas.input_schemas.clone(),
                    schemas.output_schemas.clone(),
                )?,
            }
        }
        Ok(())
    }
}
