use crate::channels::ProcessorChannelForwarder;
use crate::epoch::Epoch;
use crate::event::EventHub;

use dozer_types::errors::internal::BoxedError;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::OpIdentifier;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::tonic::async_trait;
use dozer_types::types::{Schema, TableOperation};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::mpsc::Sender;

pub use dozer_types::types::PortHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum OutputPortType {
    Stateless,
    StatefulWithPrimaryKeyLookup,
}

impl Display for OutputPortType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputPortType::Stateless => f.write_str("Stateless"),
            OutputPortType::StatefulWithPrimaryKeyLookup { .. } => {
                f.write_str("StatefulWithPrimaryKeyLookup")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutputPortDef {
    pub handle: PortHandle,
    pub typ: OutputPortType,
}

impl OutputPortDef {
    pub fn new(handle: PortHandle, typ: OutputPortType) -> Self {
        Self { handle, typ }
    }
}

pub trait SourceFactory: Send + Sync + Debug {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError>;
    fn get_output_port_name(&self, port: &PortHandle) -> String;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(
        &self,
        output_schemas: HashMap<PortHandle, Schema>,
        event_hub: EventHub,
        state: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError>;
}

#[async_trait]
pub trait Source: Send + Sync + Debug {
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError>;

    async fn start(
        &mut self,
        sender: Sender<(PortHandle, IngestionMessage)>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError>;
}

#[async_trait]
pub trait ProcessorFactory: Send + Sync + Debug {
    async fn get_output_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
        event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError>;
    fn type_name(&self) -> String;
    fn id(&self) -> String;
}

pub trait Processor: Send + Sync + Debug {
    fn commit(&self, epoch_details: &Epoch) -> Result<(), BoxedError>;
    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError>;
}

#[async_trait]
pub trait SinkFactory: Send + Sync + Debug {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_input_port_name(&self, port: &PortHandle) -> String;
    fn prepare(&self, input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError>;
    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        event_hub: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError>;
    fn type_name(&self) -> String;
}

pub trait Sink: Send + Sync + Debug {
    fn commit(&mut self, epoch_details: &Epoch) -> Result<(), BoxedError>;
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError>;

    fn on_source_snapshotting_started(&mut self, connection_name: String)
        -> Result<(), BoxedError>;
    fn on_source_snapshotting_done(
        &mut self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError>;

    // Pipeline state management.
    fn set_source_state(&mut self, source_state: &[u8]) -> Result<(), BoxedError>;
    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError>;
    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError>;

    fn preferred_batch_size(&self) -> Option<u64> {
        None
    }

    fn max_batch_duration_ms(&self) -> Option<u64> {
        None
    }

    /// If the Sink batches operations, flush the batch to the store when this method is called.
    /// This method is guaranteed to only be called on commit boundaries
    fn flush_batch(&mut self) -> Result<(), BoxedError> {
        Ok(())
    }
}
