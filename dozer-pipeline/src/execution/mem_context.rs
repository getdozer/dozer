use crate::ExecutionContext;

pub struct MemoryExecutionContext {

}

impl MemoryExecutionContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExecutionContext for MemoryExecutionContext {

}


