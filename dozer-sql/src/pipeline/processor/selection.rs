use crate::pipeline::expression::operator::Expression;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::node::{ChannelForwarder, ExecutionContext, Processor};
use dozer_types::types::{Field, Operation, OperationEvent};
use num_traits::FloatErrorKind::Invalid;

pub struct SelectionProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
    operator: Box<dyn Expression>,
}

impl SelectionProcessor {
    pub fn new(
        id: i32,
        input_ports: Option<Vec<PortHandle>>,
        output_ports: Option<Vec<PortHandle>>,
        operator: Box<dyn Expression>,
    ) -> Self {
        Self {
            id,
            input_ports,
            output_ports,
            operator,
        }
    }
}

impl Processor for SelectionProcessor {
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
        //println!("PROC {}: Message {} received", self.id, op.id);

        match op.operation {
            Operation::Delete { old } => {
                Err("DELETE Operation not supported.".to_string())
            }
            Operation::Insert { ref new} => {
                if self.operator.get_result(&new) == Field::Boolean(true) {
                    fw.send(op, None);
                }
                Ok(NextStep::Continue)
            }
            Operation::Update { old, new} => Err("UPDATE Operation not supported.".to_string()),
            Operation::Terminate => Err("TERMINATE Operation not supported.".to_string()),
            _ => Err("TERMINATE Operation not supported.".to_string()),
        }
    }
}
