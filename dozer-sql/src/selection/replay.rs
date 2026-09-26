use std::sync::{Arc, Mutex};

use dozer_sql_expression::execution::Expression;

use crate::errors::PipelineError;

/// A downstream stateful consumer can require an already planned relation to
/// reproduce the same values when an old row is deleted or updated.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReplayRequirement(Arc<Mutex<ReplayPolicy>>);

#[derive(Debug, Default)]
struct ReplayPolicy {
    required: bool,
    unsupported: Option<String>,
}

impl ReplayRequirement {
    pub(crate) fn require(&self) {
        self.0.lock().unwrap().required = true;
    }

    pub(crate) fn is_required(&self) -> bool {
        self.0.lock().unwrap().required
    }

    pub(crate) fn reject_when_required(&self, reason: String) {
        self.0.lock().unwrap().unsupported.get_or_insert(reason);
    }

    pub(crate) fn validate(&self, expression: &Expression) -> Result<(), PipelineError> {
        let policy = self.0.lock().unwrap();
        if policy.required {
            if let Some(reason) = &policy.unsupported {
                return Err(PipelineError::InvalidQuery(reason.clone()));
            }
            super::membership::validate_operand(expression)?;
        }
        Ok(())
    }
}
