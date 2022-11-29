use crate::dag::errors::ExecutionError;
use crate::sources::errors::SourceError;
use crate::sources::subscriptions::{RelationId, SourceId, SubscriptionsManager};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use std::sync::Arc;

pub struct SourceForwarder {
    source_id: SourceId,
    sm: Arc<RwLock<SubscriptionsManager>>,
}

impl SourceForwarder {
    pub fn insert_record(
        &self,
        rel_id: RelationId,
        tx_id: u64,
        last_in_tx: bool,
        new: Record,
    ) -> Result<(), SourceError> {
        self.sm.read().forward_to_pipelines(
            &(self.source_id, rel_id),
            Arc::new(Operation::Insert { new }),
        )?;
        Ok(())
    }

    pub fn delete_record(
        &self,
        rel_id: RelationId,
        tx_id: u64,
        last_in_tx: bool,
        old: Record,
    ) -> Result<(), SourceError> {
        self.sm.read().forward_to_pipelines(
            &(self.source_id, rel_id),
            Arc::new(Operation::Delete { old }),
        )?;
        Ok(())
    }

    fn update_record(
        &self,
        rel_id: RelationId,
        tx_id: u64,
        last_in_tx: bool,
        old: Record,
        new: Record,
    ) -> Result<(), SourceError> {
        self.sm.read().forward_to_pipelines(
            &(self.source_id, rel_id),
            Arc::new(Operation::Update { old, new }),
        )?;
        Ok(())
    }
    pub fn update_schema(&self, rel_id: RelationId, schema: Schema) -> Result<(), ExecutionError> {
        Ok(())
    }
    pub fn new(source_id: SourceId, sm: Arc<RwLock<SubscriptionsManager>>) -> Self {
        Self { source_id, sm }
    }
}
