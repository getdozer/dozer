use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::dag_schemas::DagSchemaManager;
use crate::dag::errors::ExecutionError;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory, SinkFactory,
    Source, SourceFactory,
};
use dozer_types::types::{FieldDefinition, FieldType, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

struct TestUsersSourceFactory {}

impl SourceFactory for TestUsersSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("user_id".to_string(), FieldType::String, false),
                true,
                true,
            )
            .field(
                FieldDefinition::new("username".to_string(), FieldType::String, false),
                true,
                true,
            )
            .field(
                FieldDefinition::new("country_id".to_string(), FieldType::String, false),
                true,
                true,
            )
            .clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(&self, input_schemas: HashMap<PortHandle, Schema>) -> Box<dyn Source> {
        todo!()
    }
}

struct TestCountriesSourceFactory {}

impl SourceFactory for TestCountriesSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("country_id".to_string(), FieldType::String, false),
                true,
                true,
            )
            .field(
                FieldDefinition::new("country_name".to_string(), FieldType::String, false),
                true,
                true,
            )
            .clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(&self, input_schemas: HashMap<PortHandle, Schema>) -> Box<dyn Source> {
        todo!()
    }
}

struct TestJoinProcessorFactory {}

impl ProcessorFactory for TestJoinProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let mut joined: Vec<FieldDefinition> = Vec::new();
        joined.extend(input_schemas.get(&1).unwrap().fields.clone());
        joined.extend(input_schemas.get(&2).unwrap().fields.clone());
        Ok(Schema {
            fields: joined,
            values: vec![],
            primary_index: vec![],
            identifier: None,
        })
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![1, 2]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Box<dyn Processor> {
        todo!()
    }
}

struct TestSinkFactory {}

impl SinkFactory for TestSinkFactory {
    fn set_input_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(&self, input_schemas: HashMap<PortHandle, Schema>) -> Box<dyn crate::dag::node::Sink> {
        todo!()
    }
}

#[test]
fn test_extract_dag_schemas() {
    let mut dag = Dag::new();

    dag.add_node(
        NodeType::Source(Arc::new(TestUsersSourceFactory {})),
        "users".to_string(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(TestCountriesSourceFactory {})),
        "countries".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(TestJoinProcessorFactory {})),
        "join".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(TestSinkFactory {})),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("users".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("join".to_string(), 1),
    ));
    chk!(dag.connect(
        Endpoint::new("countries".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("join".to_string(), 2),
    ));
    chk!(dag.connect(
        Endpoint::new("join".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), DEFAULT_PORT_HANDLE),
    ));

    let sm = chk!(DagSchemaManager::new(&dag));

    let users_output = chk!(sm.get_node_output_schemas(&"users".to_string()));
    assert_eq!(
        users_output.get(&DEFAULT_PORT_HANDLE).unwrap().fields.len(),
        3
    );

    let countries_output = chk!(sm.get_node_output_schemas(&"countries".to_string()));
    assert_eq!(
        countries_output
            .get(&DEFAULT_PORT_HANDLE)
            .unwrap()
            .fields
            .len(),
        2
    );

    let join_input = chk!(sm.get_node_input_schemas(&"join".to_string()));
    assert_eq!(join_input.get(&1).unwrap().fields.len(), 3);
    assert_eq!(join_input.get(&2).unwrap().fields.len(), 2);

    let join_output = chk!(sm.get_node_output_schemas(&"join".to_string()));
    assert_eq!(
        join_output.get(&DEFAULT_PORT_HANDLE).unwrap().fields.len(),
        5
    );

    let sink_input = chk!(sm.get_node_input_schemas(&"sink".to_string()));
    assert_eq!(
        sink_input.get(&DEFAULT_PORT_HANDLE).unwrap().fields.len(),
        5
    );
}

#[test]
fn test_init_metadata() {
    let mut dag = Dag::new();
    dag.add_node(
        NodeType::Source(Arc::new(TestUsersSourceFactory {})),
        "users".to_string(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(TestCountriesSourceFactory {})),
        "countries".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(TestJoinProcessorFactory {})),
        "join".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(TestSinkFactory {})),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("users".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("join".to_string(), 1),
    ));
    chk!(dag.connect(
        Endpoint::new("countries".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("join".to_string(), 2),
    ));
    chk!(dag.connect(
        Endpoint::new("join".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), DEFAULT_PORT_HANDLE),
    ));

    let tmp_dir = chk!(TempDir::new("example"));
    let _exec = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));
    let _exec = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    let mut dag = Dag::new();
    dag.add_node(
        NodeType::Source(Arc::new(TestUsersSourceFactory {})),
        "users".to_string(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(TestUsersSourceFactory {})),
        "countries".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(TestJoinProcessorFactory {})),
        "join".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(TestSinkFactory {})),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("users".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("join".to_string(), 1),
    ));
    chk!(dag.connect(
        Endpoint::new("countries".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("join".to_string(), 2),
    ));
    chk!(dag.connect(
        Endpoint::new("join".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), DEFAULT_PORT_HANDLE),
    ));

    let exec = DagExecutor::new(&dag, tmp_dir.path(), ExecutorOptions::default());
    assert!(exec.is_err());
}
