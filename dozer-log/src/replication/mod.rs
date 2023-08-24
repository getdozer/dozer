use std::future::Future;
use std::ops::{DerefMut, Range};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use camino::Utf8Path;
use dozer_types::grpc_types::internal::storage_response;
use dozer_types::log::{debug, error};
use dozer_types::parking_lot::Mutex;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::Operation;
use dozer_types::{bincode, thiserror};
use pin_project::pin_project;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinHandle;

use crate::storage::{Queue, Storage};

use self::persist::{load_persisted_log_entries, persisted_log_entries_end};

pub use self::persist::create_data_storage;

mod persist;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct PersistedLogEntry {
    pub key: String,
    pub range: Range<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Non-empty writable log")]
    NonEmptyWritableLog,
    #[error("Storage error: {0}")]
    Storage(#[from] super::storage::Error),
    #[error("Unrecognized log entry: {0}")]
    UnrecognizedLogEntry(String),
    #[error("First log entry does not start from zero: {0:?}")]
    FirstLogEntryNotStartFromZero(PersistedLogEntry),
    #[error("Log entry is not consecutive: {0:?}, {1:?}")]
    LogEntryNotConsecutive(PersistedLogEntry, PersistedLogEntry),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Persisting thread has quit")]
    PersistingThreadQuit,
}

/// Invariant:
/// - persisted.is_empty() || persisted[0].range.start == 0
/// - persisted[i + 1].range.start == persisted[i].range.end
/// - persisted.is_empty() || persisted.last().range.end == in_memory_offset
/// - watchers[i].start >= in_memory_offset && watchers[i].end > in_memory_offset + in_memory.len()
#[derive(Debug)]
pub struct Log {
    persisted: Vec<PersistedLogEntry>,
    in_memory: InMemoryLog,
    next_watcher_id: WatcherId,
    /// There'll only be a few watchers so we don't use HashMap.
    watchers: Vec<Watcher>,
    storage: storage_response::Storage,
    prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WatcherId(u64);

#[derive(Debug)]
struct Watcher {
    id: WatcherId,
    request: Range<usize>,
    timeout: bool,
    /// Only `None` after the watcher is triggered.
    sender: Option<tokio::sync::oneshot::Sender<Vec<LogOperation>>>,
}

#[derive(Debug, Clone)]
struct InMemoryLog {
    /// Index of the first element in `ops` in the whole log.
    start: usize,
    ops: Vec<LogOperation>,
    /// Index in `ops` where the next persisting should start.
    next_persist_start: usize,
}

impl Log {
    pub fn describe_storage(&self) -> storage_response::Storage {
        self.storage.clone()
    }

    pub async fn new(
        storage: &dyn Storage,
        mut prefix: String,
        readonly: bool,
    ) -> Result<Self, Error> {
        if !readonly {
            // Right now we don't support appending to an existing log, because the pipeline doesn't support restart yet.
            // So we write a marker file to every log to mark it as "dirty" and should not be reused.
            let marker_file = AsRef::<Utf8Path>::as_ref(&prefix).join("marker");
            if !storage
                .list_objects(marker_file.clone().into(), None)
                .await?
                .objects
                .is_empty()
            {
                return Err(Error::NonEmptyWritableLog);
            }
            storage.put_object(marker_file.into(), vec![]).await?;
        }

        prefix = AsRef::<Utf8Path>::as_ref(&prefix).join("data").into();
        let persisted = load_persisted_log_entries(storage, prefix.clone()).await?;
        let end = persisted_log_entries_end(&persisted);

        let in_memory = InMemoryLog {
            start: end.unwrap_or(0),
            ops: vec![],
            next_persist_start: 0,
        };
        let watchers = vec![];
        let storage_description = storage.describe();
        Ok(Self {
            persisted,
            in_memory,
            next_watcher_id: WatcherId(0),
            watchers,
            storage: storage_description,
            prefix,
        })
    }

    pub fn write(&mut self, op: LogOperation) {
        // Record operation.
        self.in_memory.ops.push(op);

        // Check watchers.
        // If watcher.end == self.mutable.end() or after timeout and have some data, send the operations and remove the watcher.
        self.watchers.retain_mut(|watcher| {
            debug_assert!(!watcher.request.is_empty());
            debug_assert!(self.in_memory.start <= watcher.request.start);
            if watcher.request.end == self.in_memory.end()
                || (self.in_memory.contains(watcher.request.start) && watcher.timeout)
            {
                trigger_watcher(watcher, &self.in_memory);
                false
            } else {
                true
            }
        });
    }

    pub fn persist(
        &mut self,
        queue: &Queue,
        this: Arc<Mutex<Log>>,
        runtime: &Runtime,
    ) -> Result<JoinHandle<Result<(), Error>>, Error> {
        debug!(
            "A new log entry should be persisted, in memory start={}, end={}",
            self.in_memory.start,
            self.in_memory.end()
        );

        // Persist this entry.
        let ops = &self.in_memory.ops[self.in_memory.next_persist_start..];
        let start = self.in_memory.start + self.in_memory.next_persist_start;
        let end = self.in_memory.end();
        let range = start..end;
        let persist_future = persist::persist(queue, &self.prefix, range.clone(), ops)?;
        self.in_memory.next_persist_start = self.in_memory.ops.len();

        // Spawn a future that awaits for persisting completion and removes in memory ops.
        Ok(runtime.spawn(async move {
            let key = match persist_future.await {
                Ok(key) => key,
                Err(_) => {
                    return Err(Error::PersistingThreadQuit);
                }
            };

            let mut this = this.lock();
            let this = this.deref_mut();
            debug_assert!(persisted_log_entries_end(&this.persisted).unwrap_or(0) == range.start);
            debug_assert!(this.in_memory.start == range.start);

            // Remove all watchers that want part of this entry so it can be removed from in memory log.
            this.watchers.retain_mut(|watcher| {
                if this.in_memory.contains(watcher.request.start) {
                    trigger_watcher(watcher, &this.in_memory);
                    false
                } else {
                    true
                }
            });

            // Add persisted entry and remove in memory ops.
            this.persisted.push(PersistedLogEntry {
                key,
                range: range.clone(),
            });
            this.in_memory.start = range.end;
            this.in_memory.ops.drain(0..range.len());
            this.in_memory.next_persist_start -= range.len();

            Ok(())
        }))
    }

    /// Returned `LogResponse` is guaranteed to contain `request.start`, but may actually contain less then `request.end`.
    pub fn read(
        &mut self,
        request: Range<usize>,
        timeout: Duration,
        this: Arc<Mutex<Log>>,
    ) -> LogResponseFuture {
        if request.is_empty() {
            return LogResponseFuture::Ready(vec![]);
        }

        // If start falls in persisted range, return persisted data.
        for persisted in &self.persisted {
            if persisted.range.contains(&request.start) {
                debug!(
                    "Sending persisted log entry key {}, range {:?} for request {request:?}",
                    persisted.key, persisted.range
                );
                return LogResponseFuture::Persisted(persisted.clone());
            }
        }

        // If end falls in memory, return in memory data.
        debug_assert!(self.in_memory.start <= request.start);
        if self.in_memory.end() >= request.end {
            return LogResponseFuture::Ready(self.in_memory.clone_range(&request));
        }

        // Otherwise add watcher.
        let (watcher_id, receiver) = self.add_watcher(request);
        tokio::spawn(async move {
            // Try to trigger watcher when timeout.
            tokio::time::sleep(timeout).await;
            let mut this = this.lock();
            let this = this.deref_mut();
            // Find the watcher. It may have been triggered by slice fulfillment or persisting.
            if let Some((index, watcher)) = this
                .watchers
                .iter_mut()
                .enumerate()
                .find(|(_, watcher)| watcher.id == watcher_id)
            {
                debug_assert!(this.in_memory.start <= watcher.request.start);
                if this.in_memory.contains(watcher.request.start) {
                    // If there's any data, trigger the watcher.
                    trigger_watcher(watcher, &this.in_memory);
                    this.watchers.remove(index);
                } else {
                    // Otherwise set the timeout flag and let next write trigger the watcher.
                    watcher.timeout = true;
                }
            }
        });

        LogResponseFuture::Watching(receiver)
    }

    fn add_watcher(
        &mut self,
        request: Range<usize>,
    ) -> (WatcherId, tokio::sync::oneshot::Receiver<Vec<LogOperation>>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let id = self.next_watcher_id;
        let watcher = Watcher {
            id,
            request,
            timeout: false,
            sender: Some(sender),
        };
        self.next_watcher_id.0 += 1;
        self.watchers.push(watcher);
        (id, receiver)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub enum LogOperation {
    Op { op: Operation },
    Commit { decision_instant: SystemTime },
    SnapshottingDone { connection_name: String },
    Terminate,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum LogResponse {
    Persisted(PersistedLogEntry),
    Operations(Vec<LogOperation>),
}

#[pin_project(project = LogResponseFutureProj)]
pub enum LogResponseFuture {
    Persisted(PersistedLogEntry),
    Ready(Vec<LogOperation>),
    Watching(#[pin] tokio::sync::oneshot::Receiver<Vec<LogOperation>>),
}

impl Future for LogResponseFuture {
    type Output = Result<LogResponse, RecvError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::{
            mem::swap,
            task::Poll::{Pending, Ready},
        };
        let this = self.project();
        match this {
            LogResponseFutureProj::Persisted(entry) => {
                Ready(Ok(LogResponse::Persisted(entry.clone())))
            }
            LogResponseFutureProj::Ready(ops) => {
                let mut result = vec![];
                swap(&mut result, ops);
                Ready(Ok(LogResponse::Operations(result)))
            }
            LogResponseFutureProj::Watching(receiver) => match receiver.poll(cx) {
                Ready(Ok(ops)) => Ready(Ok(LogResponse::Operations(ops))),
                Ready(Err(e)) => Ready(Err(e)),
                Pending => Pending,
            },
        }
    }
}

fn trigger_watcher(watcher: &mut Watcher, log: &InMemoryLog) {
    let sender = watcher.sender.take().expect("watcher already triggered");
    watcher.request.end = watcher.request.end.min(log.end());
    let result = log.clone_range(&watcher.request);
    if sender.send(result).is_err() {
        error!("Watcher future dropped before triggering");
    }
}

impl InMemoryLog {
    fn end(&self) -> usize {
        self.start + self.ops.len()
    }

    fn contains(&self, start: usize) -> bool {
        self.start <= start && start < self.end()
    }

    fn clone_range(&self, range: &Range<usize>) -> Vec<LogOperation> {
        debug!(
            "Cloning range: {:?}, self.start={}, self.end={}",
            range,
            self.start,
            self.end()
        );
        self.ops[range.start - self.start..range.end - self.start].to_vec()
    }
}

#[cfg(test)]
mod tests;
