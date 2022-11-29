use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::ExecutorOperation;
use crate::sources::errors::SourceError;
use crate::sources::source_forwarder::SourceForwarder;
use crate::sources::subscriptions::{
    RelationId, RelationName, SourceId, SourceName, SubscriptionsManager,
};
use crate::storage::record_reader::RecordReader;
use crossbeam::channel::Sender;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, OperationEvent, Record, Schema};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct SourceOptions {
    id: SourceId,
    name: SourceName,
    relations: Vec<RelationOptions>,
    retr_old_record_for_updates: bool,
    retr_old_record_for_deletes: bool,
}

pub struct RelationOptions {
    id: RelationId,
    name: RelationName,
}

pub trait SourceIngestor: Send + Sync {
    fn start(
        &self,
        settings: &SourceOptions,
        fw: &SourceForwarder,
        stop_req: Arc<AtomicBool>,
    ) -> Result<(), SourceError>;
}

pub struct Source {
    settings: SourceOptions,
    subscriptions: Arc<RwLock<SubscriptionsManager>>,
    relation_idx: HashMap<RelationName, RelationId>,
    stop_req: Arc<AtomicBool>,
    ingest_thread: Option<JoinHandle<Result<(), SourceError>>>,
    ingest_runner: Box<dyn SourceIngestor>,
}

impl Source {
    pub fn new(
        id: SourceId,
        name: SourceName,
        settings: SourceOptions,
        subscriptions: Arc<RwLock<SubscriptionsManager>>,
        ingest_runner: Box<dyn SourceIngestor>,
    ) -> Self {
        let relation_idx: HashMap<RelationName, RelationId> = settings
            .relations
            .iter()
            .map(|e| (e.name.clone(), e.id.clone()))
            .collect();

        Self {
            relation_idx,
            subscriptions,
            ingest_runner,
            ingest_thread: None,
            stop_req: Arc::new(AtomicBool::new(false)),
            settings,
        }
    }

    pub fn start(&mut self) {
        self.ingest_thread = Some(thread::spawn(move || -> Result<(), SourceError> {
            let fw = SourceForwarder::new(self.id, self.subscriptions.clone());
            self.ingest_runner
                .start(&self.settings, &fw, self.stop_req.clone())
        }));
    }
}
