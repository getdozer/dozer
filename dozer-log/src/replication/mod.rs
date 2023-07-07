use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dozer_types::epoch::ExecutorOperation;
use dozer_types::grpc_types::internal::storage_response;
use dozer_types::log::{debug, error};
use dozer_types::models::app_config::LogStorage;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::{bincode, thiserror};
use pin_project::pin_project;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinHandle};

use crate::home_dir::BuildPath;

use self::persist::{load_persisted_log_entries, persisted_log_entries_end, PersistingQueue};

pub use self::persist::create_log_storage;

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
    #[error("Persisting thread has quit: {0:?}")]
    PersistingThreadQuit(#[source] Option<JoinError>),
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    pub storage_config: LogStorage,
    pub entry_max_size: usize,
    pub max_num_immutable_entries: usize,
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
    watchers: Vec<Watcher>,
    queue: PersistingQueue,
    storage: storage_response::Storage,
    entry_max_size: usize,
}

#[derive(Debug)]
struct Watcher {
    request: Range<usize>,
    deadline: Instant,
    /// Only `None` after the watcher is triggered.
    sender: Option<tokio::sync::oneshot::Sender<Vec<ExecutorOperation>>>,
}

#[derive(Debug, Clone)]
struct InMemoryLog {
    start: usize,
    ops: Vec<ExecutorOperation>,
}

impl Log {
    pub fn describe_storage(&self) -> storage_response::Storage {
        self.storage.clone()
    }

    pub async fn new(
        options: LogOptions,
        build_path: &BuildPath,
        readonly: bool,
    ) -> Result<Self, Error> {
        let (storage, prefix) = create_log_storage(options.storage_config, build_path).await?;
        let persisted = load_persisted_log_entries(&*storage, prefix.clone()).await?;
        let end = persisted_log_entries_end(&persisted);
        if !readonly && end.is_some() {
            return Err(Error::NonEmptyWritableLog);
        }

        let in_memory = InMemoryLog {
            start: end.unwrap_or(0),
            ops: vec![],
        };
        let watchers = vec![];
        let storage_description = storage.describe();
        let queue =
            PersistingQueue::new(storage, prefix, options.max_num_immutable_entries).await?;
        Ok(Self {
            persisted,
            in_memory,
            watchers,
            queue,
            storage: storage_description,
            entry_max_size: options.entry_max_size,
        })
    }

    pub async fn write(
        &mut self,
        op: ExecutorOperation,
        this: Arc<Mutex<Log>>,
    ) -> Result<Option<JoinHandle<()>>, Error> {
        // Record operation.
        self.in_memory.ops.push(op);

        // Check watchers.
        // If watcher.end == self.mutable.end() or after deadline and have some data, send the operations and remove the watcher.
        let now = Instant::now();
        self.watchers.retain_mut(|watcher| {
            debug_assert!(!watcher.request.is_empty());
            debug_assert!(self.in_memory.start <= watcher.request.start);
            if watcher.request.end == self.in_memory.end()
                || (self.in_memory.contains(watcher.request.start) && now > watcher.deadline)
            {
                trigger_watcher(watcher, &self.in_memory);
                false
            } else {
                true
            }
        });

        if self.in_memory.ops.len() % self.entry_max_size == 0 {
            debug!(
                "A new log entry should be persisted, in memory start={}, end={}",
                self.in_memory.start,
                self.in_memory.end()
            );
            // Remove all watchers that want part of this entry so it can be removed from in memory log in the future.
            self.watchers.retain_mut(|watcher| {
                if self.in_memory.contains(watcher.request.start) {
                    trigger_watcher(watcher, &self.in_memory);
                    false
                } else {
                    true
                }
            });

            // Persist this entry.
            let ops = self.in_memory.ops[self.in_memory.ops.len() - self.entry_max_size..].to_vec();
            let end = self.in_memory.end();
            let start = end - self.entry_max_size;
            let range = start..end;
            let persist_future = self.queue.persist(range.clone(), ops).await?;

            // Spawn a future that awaits for persisting completion and removes in memory ops.
            Ok(Some(tokio::spawn(async move {
                let key = match persist_future.await {
                    Ok(key) => key,
                    Err(_) => {
                        error!("{}", Error::PersistingThreadQuit(None));
                        return;
                    }
                };

                let persisted_log_entry = PersistedLogEntry {
                    key,
                    range: range.clone(),
                };
                let mut this = this.lock().await;
                debug_assert!(
                    persisted_log_entries_end(&this.persisted).unwrap_or(0) == range.start
                );
                debug_assert!(this.in_memory.start == range.start);
                this.persisted.push(persisted_log_entry);
                this.in_memory.start = range.end;
                this.in_memory.ops.drain(0..range.len());
            })))
        } else {
            Ok(None)
        }
    }

    /// Returned `LogResponse` is guaranteed to contain `request.start`, but may actually contain less then `request.end`.
    pub fn read(&mut self, request: Range<usize>, timeout: Duration) -> LogResponseFuture {
        if request.is_empty() {
            return LogResponseFuture::Ready(vec![]);
        }

        // If start falls in persisted range, return persisted data.
        for persisted in &self.persisted {
            if persisted.range.contains(&request.start) {
                return LogResponseFuture::Persisted(persisted.clone());
            }
        }

        // If end falls in memory, return in memory data.
        debug_assert!(self.in_memory.start <= request.start);
        if self.in_memory.end() >= request.end {
            return LogResponseFuture::Ready(self.in_memory.clone_range(&request));
        }

        // Otherwise add watcher.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.watchers.push(Watcher {
            request,
            deadline: Instant::now() + timeout,
            sender: Some(sender),
        });
        LogResponseFuture::Watching(receiver)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum LogResponse {
    Persisted(PersistedLogEntry),
    Operations(Vec<ExecutorOperation>),
}

#[pin_project(project = LogResponseFutureProj)]
pub enum LogResponseFuture {
    Persisted(PersistedLogEntry),
    Ready(Vec<ExecutorOperation>),
    Watching(#[pin] tokio::sync::oneshot::Receiver<Vec<ExecutorOperation>>),
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

    fn clone_range(&self, range: &Range<usize>) -> Vec<ExecutorOperation> {
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
