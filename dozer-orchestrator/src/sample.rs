use dozer_core::dag::{
    dag::PortHandle,
    node::{ChannelForwarder, ExecutionContext, NextStep, Processor, Sink},
};
use dozer_shared::types::OperationEvent;

pub struct SampleSink {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
}

impl SampleSink {
    pub fn new(id: i32, input_ports: Option<Vec<PortHandle>>) -> Self {
        Self { id, input_ports }
    }
}

impl Sink for SampleSink {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn init(&self) -> Result<(), String> {
        println!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(
        &self,
        from_port: Option<PortHandle>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
    ) -> Result<NextStep, String> {
        if op.id % 1000 == 0 {
            println!("Sampled Event from Sink: {:?}", op);
        }
        Ok(NextStep::Continue)
    }
}

pub struct SampleProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
}

impl SampleProcessor {
    pub fn new(
        id: i32,
        input_ports: Option<Vec<PortHandle>>,
        output_ports: Option<Vec<PortHandle>>,
    ) -> Self {
        Self {
            id,
            input_ports,
            output_ports,
        }
    }
}

impl Processor for SampleProcessor {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> Result<(), String> {
        println!("PROC {}: Initialising TestProcessor", self.id);
        Ok(())
    }

    fn process(
        &self,
        from_port: Option<PortHandle>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
        fw: &ChannelForwarder,
    ) -> Result<NextStep, String> {
        //  println!("PROC {}: Message {} received", self.id, op.id);
        fw.send(op, None).unwrap();
        Ok(NextStep::Continue)
    }
}
