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
pub type SourceRelationId = u32;
pub type SourceRelationName = String;
pub type RelationUniqueName = String;
pub type SourceId = u16;
pub type SourceName = String;
pub type RelationSourceName = (SourceName, SourceRelationName);
pub type RelationSourceId = (SourceId, SourceRelationId);

pub struct SubscriptionsManager {
    rel_name_idx: HashMap<RelationUniqueName, RelationSourceId>,
    subscribers: HashMap<RelationSourceId, HashMap<PipelineId, Arc<Sender<Arc<Operation>>>>>,
}

impl SubscriptionsManager {
    pub fn subscribe(
        &mut self,
        from_pipeline: PipelineId,
        subscribers: HashMap<RelationUniqueName, Arc<Sender<Arc<Operation>>>>,
    ) -> Result<(), SourceError> {
        for (unique_name, sender) in subscribers {
            let id = self
                .rel_name_idx
                .get(&unique_name)
                .ok_or(SourceError::InvalidRelation(unique_name))?;
            let mut subs = self.subscribers.entry(id.clone()).or_insert(HashMap::new());
            subs.insert(from_pipeline.clone(), sender);
        }
        Ok(())
    }

    pub fn unsubscribe(&mut self, from_pipeline: PipelineId) {
        for (rel_src_id, mut subs) in &mut self.subscribers {
            subs.remove(&from_pipeline);
        }
    }

    pub fn register(&mut self, src_rel: RelationSourceId, unique_name: RelationUniqueName) {
        self.rel_name_idx.insert(unique_name, src_rel);
    }

    pub fn forward(&self, from: &RelationSourceId, op: Arc<Operation>) -> Result<(), SourceError> {
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
