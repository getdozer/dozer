use dozer_core::dag::channels::ProcessorChannelForwarder;
use std::collections::HashMap;

use dozer_core::dag::epoch::Epoch;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory,
};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::types::Schema;

#[derive(Debug)]
pub struct BasicProcessorFactory {}

impl BasicProcessorFactory {
    /// Creates a new [`BasixProcessorFactory`].
    pub fn new() -> Self {
        Self {}
    }
}

pub(crate) const BASIC_PROCESSOR_INPUT_PORT: PortHandle = 0xffff_u16;
pub(crate) const BASIC_PROCESSOR_OUTPUT_PORT: PortHandle = 0xffff_u16;

impl ProcessorFactory<SchemaSQLContext> for BasicProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        Ok(input_schemas
            .get(&BASIC_PROCESSOR_INPUT_PORT)
            .unwrap()
            .clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![BASIC_PROCESSOR_INPUT_PORT]
    }
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            BASIC_PROCESSOR_OUTPUT_PORT,
            OutputPortType::StatefulWithPrimaryKeyLookup {
                retr_old_records_for_deletes: true,
                retr_old_records_for_updates: true,
            },
        )]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(BasicProcessor {}))
    }
}

#[derive(Debug)]
pub(crate) struct BasicProcessor {}

impl Processor for BasicProcessor {
    fn init(&mut self, _tx: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(
        &self,
        _epoch_details: &Epoch,
        _tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: dozer_types::types::Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _tx: &SharedTransaction,
        _readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, BASIC_PROCESSOR_OUTPUT_PORT)
    }
}
