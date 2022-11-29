use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::ExecutorOperation;
use crate::sources::errors::SourceError;
use crate::sources::subscriptions::{RelationId, SourceId, SubscriptionsManager};
use crate::storage::record_reader::RecordReader;
use crossbeam::channel::Sender;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, OperationEvent, Record, Schema};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct RelationsSourceSettings {}

pub trait RelationsSourceIngestor {
    fn start(
        &self,
        settings: &RelationsSourceSettings,
        fw: RelationsSourceForwarder,
        stop_req: Arc<AtomicBool>,
    ) -> Result<(), ExecutionError>;
}

struct RelationSubscriptions {
    subs: RwLock<HashMap<u32, Sender<ExecutorOperation>>>,
}

impl RelationSubscriptions {}

pub struct RelationsSourceForwarder {
    source_id: SourceId,
    sm: Arc<RwLock<SubscriptionsManager>>,
}

impl RelationsSourceForwarder {
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
}

pub struct RelationsSource {
    mappings: HashMap<String, u32>,
    status: Option<JoinHandle<Result<(), ExecutionError>>>,
    stop_req: Arc<AtomicBool>,
    runner: Box<dyn RelationsSourceIngestor>,
}

impl RelationsSource {
    pub fn new(mappings: HashMap<String, u32>, runner: Box<dyn RelationsSourceIngestor>) -> Self {
        Self {
            mappings,
            runner,
            status: None,
            stop_req: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start() {}
}
