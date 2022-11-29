use crate::dag::executor_local::ExecutorOperation;
use crate::sources::errors::SourceError;
use crate::sources::errors::SourceError::ForwardError;
use crossbeam::channel::Sender;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, OperationEvent};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

pub type PipelineId = String;
pub type RelationId = u32;
pub type RelationName = String;
pub type SourceId = u16;
pub type SourceName = String;
pub type RelationSourceName = (SourceName, RelationName);
pub type RelationSourceId = (SourceId, RelationId);

pub(crate) struct SubscriptionsManager {
    rel_name_idx: HashMap<RelationSourceName, RelationSourceId>,
    subscribers: HashMap<RelationSourceId, HashMap<PipelineId, Rc<Sender<Arc<Operation>>>>>,
    subscribed_pipelines:
        HashMap<PipelineId, HashMap<RelationSourceName, Rc<Sender<Arc<Operation>>>>>,
}

impl SubscriptionsManager {
    pub fn subscribe_to_sources(
        &mut self,
        from_pipeline: PipelineId,
        subscribers: HashMap<RelationSourceName, Rc<Sender<Arc<Operation>>>>,
    ) -> Result<(), SourceError> {
        for e in subscribers {
            let id = self
                .rel_name_idx
                .get(&e.0)
                .ok_or(SourceError::InvalidSource(e.0 .0.clone(), e.0 .1.clone()))?;
            let mut subs = self.subscribers.entry(id.clone()).or_insert(HashMap::new());
            subs.insert(from_pipeline.clone(), e.1);
        }
        Ok(())
    }

    pub fn forward_to_pipelines(
        &self,
        from: &RelationSourceId,
        op: Arc<Operation>,
    ) -> Result<(), SourceError> {
        let mut errs = Vec::<PipelineId>::new();
        if let Some(s) = self.subscribers.get(from) {
            for (p_id, sender) in s {
                if sender.send(op.clone()).is_err() {
                    errs.push(p_id.clone());
                }
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ForwardError(errs))
        }
    }
}
