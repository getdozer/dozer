use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use dozer_sql_expression::execution::Expression;

use crate::errors::PipelineError;

/// A downstream stateful consumer can require an already planned relation to
/// reproduce the same values when an old row is deleted or updated.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReplayRequirement(Arc<AtomicBool>);

impl ReplayRequirement {
    pub(crate) fn require(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub(crate) fn validate(&self, expression: &Expression) -> Result<(), PipelineError> {
        if self.0.load(Ordering::Relaxed) {
            super::membership::validate_operand(expression)?;
        }
        Ok(())
    }
}
