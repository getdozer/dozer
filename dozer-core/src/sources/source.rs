use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::ExecutorOperation;
use crate::sources::errors::SourceError;
use crate::sources::source_forwarder::SourceForwarder;
use crate::sources::subscriptions::{
    SourceId, SourceName, SourceRelationId, SourceRelationName, SubscriptionsManager,
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

#[derive(Clone, Debug)]
pub enum UpdateDeleteOpMode {
    FullRecord,
    PrimaryKeys,
}

#[derive(Clone, Debug)]
pub struct SourceSettings {
    id: SourceId,
    relations: Vec<RelationSettings>,
}

#[derive(Clone, Debug)]
pub struct RelationSettings {
    id: SourceRelationId,
    name: SourceRelationName,
    op_mode: UpdateDeleteOpMode,
}

pub trait SourceIngestor: Send + Sync {
    fn start(
        &self,
        settings: &SourceSettings,
        fw: &SourceForwarder,
        stop_req: Arc<AtomicBool>,
    ) -> Result<(), SourceError>;
}

pub struct Source {
    settings: SourceSettings,
    subscriptions: Arc<RwLock<SubscriptionsManager>>,
    relation_idx: HashMap<SourceRelationName, SourceRelationId>,
    stop_req: Arc<AtomicBool>,
    ingest_thread: Option<JoinHandle<Result<(), SourceError>>>,
    ingest_runner: Arc<dyn SourceIngestor>,
}

impl Source {
    pub fn new(
        settings: SourceSettings,
        subscriptions: Arc<RwLock<SubscriptionsManager>>,
        ingest_runner: Arc<dyn SourceIngestor>,
    ) -> Self {
        let relation_idx: HashMap<SourceRelationName, SourceRelationId> = settings
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
        let stop_req = self.stop_req.clone();
        let subscriptions = self.subscriptions.clone();
        let settings = self.settings.clone();
        let runner = self.ingest_runner.clone();

        self.ingest_thread = Some(thread::spawn(move || -> Result<(), SourceError> {
            let fw = SourceForwarder::new(settings.id, subscriptions);
            runner.start(&settings, &fw, stop_req)
        }));
    }
}
