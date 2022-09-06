use crate::ExecutionContext;

pub struct MemoryExecutionContext {

}

impl MemoryExecutionContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExecutionContext for MemoryExecutionContext {
    fn get_kv(&self, id: String) {
        println!("getting kv");
    }
}


