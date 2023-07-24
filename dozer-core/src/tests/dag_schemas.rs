use crate::dag_schemas::{DagHaveSchemas, DagSchemas};
use crate::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, SinkFactory, Source,
    SourceFactory,
};
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};

use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
use std::collections::HashMap;

use crate::tests::app::NoneContext;
use std::sync::Arc;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[derive(Debug)]
struct TestUsersSourceFactory {}

impl SourceFactory<NoneContext> for TestUsersSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<(Schema, NoneContext), BoxedError> {
        Ok((
            Schema::default()
                .field(
                    FieldDefinition::new(
                        "user_id".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .field(
                    FieldDefinition::new(
                        "username".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .field(
                    FieldDefinition::new(
                        "country_id".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .clone(),
            NoneContext {},
        ))
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        todo!()
    }
}

#[derive(Debug)]
struct TestCountriesSourceFactory {}

impl SourceFactory<NoneContext> for TestCountriesSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<(Schema, NoneContext), BoxedError> {
        Ok((
            Schema::default()
                .field(
                    FieldDefinition::new(
                        "country_id".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .field(
                    FieldDefinition::new(
                        "country_name".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .clone(),
            NoneContext {},
        ))
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        todo!()
    }
}

#[derive(Debug)]
struct TestJoinProcessorFactory {}

impl ProcessorFactory<NoneContext> for TestJoinProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
        let mut joined: Vec<FieldDefinition> = Vec::new();
        joined.extend(input_schemas.get(&1).unwrap().0.fields.clone());
        joined.extend(input_schemas.get(&2).unwrap().0.fields.clone());
        Ok((
            Schema {
                fields: joined,
                primary_index: vec![],
            },
            NoneContext {},
        ))
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![1, 2]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        todo!()
    }

    fn type_name(&self) -> String {
        "TestJoin".to_owned()
    }

    fn id(&self) -> String {
        "TestJoin".to_owned()
    }
}

#[derive(Debug)]
struct TestSinkFactory {}

impl SinkFactory<NoneContext> for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn crate::node::Sink>, BoxedError> {
        todo!()
    }
}

#[test]
fn test_extract_dag_schemas() {
    let mut dag = Dag::new();

    let users_handle = NodeHandle::new(Some(1), 1.to_string());
    let countries_handle = NodeHandle::new(Some(1), 2.to_string());
    let join_handle = NodeHandle::new(Some(1), 3.to_string());
    let sink_handle = NodeHandle::new(Some(1), 4.to_string());

    let users_index = dag.add_source(users_handle.clone(), Arc::new(TestUsersSourceFactory {}));
    let countries_index = dag.add_source(
        countries_handle.clone(),
        Arc::new(TestCountriesSourceFactory {}),
    );
    let join_index = dag.add_processor(join_handle.clone(), Arc::new(TestJoinProcessorFactory {}));
    let sink_index = dag.add_sink(sink_handle.clone(), Arc::new(TestSinkFactory {}));

    chk!(dag.connect(
        Endpoint::new(users_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(join_handle.clone(), 1),
    ));
    chk!(dag.connect(
        Endpoint::new(countries_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(join_handle.clone(), 2),
    ));
    chk!(dag.connect(
        Endpoint::new(join_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, DEFAULT_PORT_HANDLE),
    ));

    let dag_schemas = chk!(DagSchemas::new(dag));

    let users_output = dag_schemas.get_node_output_schemas(users_index);
    assert_eq!(
        users_output.get(&DEFAULT_PORT_HANDLE).unwrap().fields.len(),
        3
    );

    let countries_output = dag_schemas.get_node_output_schemas(countries_index);
    assert_eq!(
        countries_output
            .get(&DEFAULT_PORT_HANDLE)
            .unwrap()
            .fields
            .len(),
        2
    );

    let join_input = dag_schemas.get_node_input_schemas(join_index);
    assert_eq!(join_input.get(&1).unwrap().fields.len(), 3);
    assert_eq!(join_input.get(&2).unwrap().fields.len(), 2);

    let join_output = dag_schemas.get_node_output_schemas(join_index);
    assert_eq!(
        join_output.get(&DEFAULT_PORT_HANDLE).unwrap().fields.len(),
        5
    );

    let sink_input = dag_schemas.get_node_input_schemas(sink_index);
    assert_eq!(
        sink_input.get(&DEFAULT_PORT_HANDLE).unwrap().fields.len(),
        5
    );
}
