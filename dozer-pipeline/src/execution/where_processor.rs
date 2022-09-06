use crate::{ExecutionContext, Operation, Processor, Record};
use async_trait::async_trait;

pub struct Where {

}

impl Where {
    pub fn new() -> Where {
        Where {}
    }
}

#[async_trait]
impl Processor for Where {
    async fn process(&mut self, data: (u8, Operation), ctx: &dyn ExecutionContext) -> Vec<(u8, Operation)> {
        ctx.get_kv("ddd".to_string());
        vec![(1, Operation::insert {table: 1, record: Record::new(0, vec![])})]
    }
}

