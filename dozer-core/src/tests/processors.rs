use std::collections::HashMap;

use dozer_types::{errors::internal::BoxedError, tonic::async_trait, types::Schema};

use crate::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};

#[derive(Debug)]
pub struct ConnectivityTestProcessorFactory;

#[async_trait]
impl ProcessorFactory for ConnectivityTestProcessorFactory {
    fn type_name(&self) -> String {
        "ConnectivityTest".to_owned()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn id(&self) -> String {
        "ConnectivityTest".to_owned()
    }
}

#[derive(Debug)]
pub struct NoInputPortProcessorFactory;

#[async_trait]
impl ProcessorFactory for NoInputPortProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn type_name(&self) -> String {
        "NoInput".to_owned()
    }

    fn id(&self) -> String {
        "NoInput".to_owned()
    }
}
