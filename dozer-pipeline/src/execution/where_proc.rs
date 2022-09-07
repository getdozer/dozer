use async_trait::async_trait;
use crate::{ExecutionContext, Field, MemoryExecutionContext, Operation, Processor, Record};
use crate::execution::where_exp::{eval_operator, Operand, Operator};

pub struct Where {
    op: Box<Operator>
}

impl Where {
    pub fn new(op: Box<Operator>) -> Self {
        Self { op }
    }
}
/***
    Input port: 0
    Output port: 0
 **/

#[async_trait]
impl Processor for Where {
    async fn process(&mut self, data: (u8, Operation), ctx: &dyn ExecutionContext) -> Vec<(u8, Operation)> {

        if data.0 != 0 {
            panic!("Data received on an invalid port");
        }

        match data.1 {
            Operation::insert {table_id, new} => {
                let b = eval_operator(&self.op,&new, ctx);
                return if b {vec![(0, Operation::insert {table_id: table_id, new: new})]} else {vec![]};
            }
            Operation::delete {table_id, old} => {
                let b = eval_operator(&self.op,&old, ctx);
                return if b {vec![(0, Operation::insert {table_id: table_id, new: old})]} else {vec![]};
            }
            Operation::update {old, new, table_id} => {

                let old_ok = eval_operator(&self.op,&old, ctx);
                let new_ok = eval_operator(&self.op,&new, ctx);

                match (old_ok, new_ok) {
                    (true, true) => { return vec![(0, Operation::update {table_id, old, new})]; }
                    (true, false) => { return vec![(0, Operation::delete {table_id: table_id, old: old})]; }
                    (false, true) => { return vec![(0, Operation::insert {table_id: table_id, new: new})]; }
                    (false, false) => { return vec![] }
                }

            }
            _ => { panic!("Received invalid operation in Where processor") }
        }
    }
}

macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
  }

mod tests {
    use futures::executor::block_on;
    use crate::execution::where_exp::{Operand, Operator};
    use crate::execution::where_proc::Where;
    use crate::{Field, MemoryExecutionContext, Operation, Processor, Record};

    #[test]
    fn test_where_insert() {
        let ctx = MemoryExecutionContext::new();
        let mut w = Where::new(Box::new(
            Operator::eq(
                Operand::const_value(Field::int_field(10)),
                Operand::field_value(0)
            )
        ));

        let input = Operation::insert {
            table_id: 1,
            new: Record::new(1, vec![Field::int_field(10), Field::string_field("test1".to_string())])
        };


        let mut exp_output: Vec<(u8, Operation)> = Vec::new();
        exp_output.push(
            (0, Operation::insert { table_id: 1, new: Record::new(1, vec![Field::int_field(10), Field::string_field("test1".to_string())])})
        );

        let r = block_on(w.process((0, input), &ctx));
        assert!(matches!(r,exp_ouput));
    }



}



