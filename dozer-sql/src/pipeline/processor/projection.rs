use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::node::{ChannelForwarder, ExecutionContext, Processor};
use dozer_types::types::OperationEvent;

pub struct ProjectionProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
}

impl ProjectionProcessor {
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

impl Processor for ProjectionProcessor {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> Result<(), String> {
        println!("PROC {}: Initialising SelectionProcessor", self.id);
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
        fw.send(op, None);
        Ok(NextStep::Continue)
    }
}
