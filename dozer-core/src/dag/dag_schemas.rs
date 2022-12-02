use crate::dag::dag::{Dag, NodeType};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::InvalidNodeHandle;
use crate::dag::node::{NodeHandle, PortHandle};
use dozer_types::types::Schema;
use std::collections::{HashMap, HashSet};

pub(crate) struct NodeSchemas {
    pub input_schemas: HashMap<PortHandle, Schema>,
    pub output_schemas: HashMap<PortHandle, Schema>,
}

impl NodeSchemas {
    pub fn new() -> Self {
        Self {
            input_schemas: HashMap::new(),
            output_schemas: HashMap::new(),
        }
    }
}

pub(crate) struct DagSchemaManager<'a> {
    dag: &'a Dag,
    schemas: HashMap<NodeHandle, NodeSchemas>,
}

impl<'a> DagSchemaManager<'a> {
    fn get_port_output_schema(
        node: &NodeType,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        match node {
            NodeType::Source(src) => src.get_output_schema(output_port),
            NodeType::Processor(proc) => proc.get_output_schema(output_port, input_schemas),
            NodeType::Sink(proc) => proc.get_output_schema(output_port, input_schemas),
        }
    }

    fn get_node_input_ports(node: &NodeType) -> Vec<PortHandle> {
        match node {
            NodeType::Source(src) => vec![],
            NodeType::Processor(proc) => proc.get_input_ports(),
            NodeType::Sink(proc) => proc.get_input_ports(),
        }
    }

    fn get_node_output_ports(node: &NodeType) -> Vec<PortHandle> {
        match node {
            NodeType::Source(src) => src.get_output_ports().iter().map(|p| p.handle).collect(),
            NodeType::Processor(proc) => proc.get_output_ports().iter().map(|p| p.handle).collect(),
            NodeType::Sink(proc) => vec![],
        }
    }

    fn fill_node_output_schemas(
        dag: &Dag,
        handle: &NodeHandle,
        all_schemas: &mut HashMap<NodeHandle, NodeSchemas>,
    ) -> Result<(), ExecutionError> {
        // Get the current node
        let node = dag
            .nodes
            .get(handle)
            .ok_or(InvalidNodeHandle(handle.clone()))?;

        // Get all input schemas available for this node
        let node_input_schemas_available = HashSet::<&PortHandle>::from_iter(
            all_schemas
                .get(handle)
                .ok_or(ExecutionError::InvalidNodeHandle(handle.clone()))?
                .input_schemas
                .iter()
                .map(|e| e.0),
        );

        // get all input schemas required for this node
        let input_ports = Self::get_node_input_ports(node);
        let node_input_schemas_required = HashSet::<&PortHandle>::from_iter(&input_ports);

        // If we have all input schemas required
        if node_input_schemas_available == node_input_schemas_required {
            // Calculate the output schema for each port and insert it in the global schemas map
            for port in &Self::get_node_output_ports(node) {
                let schema = {
                    let node_schemas = all_schemas
                        .get_mut(handle)
                        .ok_or(ExecutionError::InvalidNodeHandle(handle.clone()))?;
                    let schema =
                        Self::get_port_output_schema(node, port, &node_schemas.input_schemas)?;
                    node_schemas
                        .output_schemas
                        .insert(port.clone(), schema.clone());
                    schema
                };

                // Retrieve all next nodes connected to this port
                let next_in_chain = dag
                    .edges
                    .iter()
                    .filter(|e| &e.from.node == handle && &e.from.port == port)
                    .map(|e| e.to.clone());

                for next_node_port_input in next_in_chain {
                    let next_node_schemas = all_schemas
                        .get_mut(&next_node_port_input.node)
                        .ok_or(InvalidNodeHandle(next_node_port_input.node.clone()))?;
                    next_node_schemas
                        .input_schemas
                        .insert(next_node_port_input.port, schema.clone());
                }
            }

            for next_node in HashSet::<NodeHandle>::from_iter(
                dag.edges
                    .iter()
                    .filter(|e| &e.from.node == handle)
                    .map(|e| e.to.node.clone()),
            ) {
                Self::fill_node_output_schemas(dag, &next_node, all_schemas)?;
            }
        }

        Ok(())
    }

    pub fn new(dag: &'a Dag) -> Result<Self, ExecutionError> {
        let sources: Vec<&NodeHandle> = dag
            .nodes
            .iter()
            .filter(|(h, n)| matches!(n, NodeType::Source(_)))
            .map(|e| e.0)
            .collect();

        let mut schemas: HashMap<NodeHandle, NodeSchemas> = dag
            .nodes
            .iter()
            .map(|(handle, _)| (handle.clone(), NodeSchemas::new()))
            .collect();

        for source in sources {
            Self::fill_node_output_schemas(dag, source, &mut schemas)?;
        }

        Ok(Self {
            dag,
            schemas: schemas,
        })
    }

    pub fn get_node_input_schemas(
        &self,
        handle: &NodeHandle,
    ) -> Result<&HashMap<PortHandle, Schema>, ExecutionError> {
        let node = self
            .schemas
            .get(handle)
            .ok_or(InvalidNodeHandle(handle.clone()))?;
        Ok(&node.input_schemas)
    }

    pub fn get_node_output_schemas(
        &self,
        handle: &NodeHandle,
    ) -> Result<&HashMap<PortHandle, Schema>, ExecutionError> {
        let node = self
            .schemas
            .get(handle)
            .ok_or(InvalidNodeHandle(handle.clone()))?;
        Ok(&node.output_schemas)
    }
}
