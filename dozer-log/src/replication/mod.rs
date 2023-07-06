use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use dozer_types::epoch::ExecutorOperation;
use dozer_types::log::{debug, error};
use dozer_types::{bincode, thiserror};
use pin_project::pin_project;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::storage::Storage;

use self::entry::{persist, LogEntry, PersistedLogEntries};

mod entry;

#[derive(Debug, Clone, PartialEq)]
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
}

/// Invariant:
/// - persisted.objects.is_empty() || (immutable.is_empty() && persisted.objects.last().range.end == mutable.start) || (!immutable.is_empty() && persisted.objects.last().range.end == immutable[0].start)
/// - immutable[i + 1].range.start == immutable[i].range.start + immutable.ops.len()
/// - immutable.is_empty() || mutable.start == immutable.last().range.start + immutable.last().ops.len()
/// - mutable.ops.len() < entry_max_size
/// - watchers[i].start >= mutable.start && watchers[i].end > mutable.start + mutable.ops.len()
#[derive(Debug)]
pub struct Log<S: Storage> {
    persisted: PersistedLogEntries<S>,
    immutable: Vec<LogEntry>,
    mutable: LogEntry,
    watchers: Vec<Watcher>,
    entry_max_size: usize,
}

#[derive(Debug)]
struct Watcher {
    request: Range<usize>,
    /// Only `None` after the watcher is triggered.
    sender: Option<tokio::sync::oneshot::Sender<Vec<ExecutorOperation>>>,
}

impl<S: Storage> Log<S> {
    pub async fn new(
        storage: S,
        migration_dir: String,
        readonly: bool,
        entry_max_size: usize,
    ) -> Result<Self, Error> {
        let persisted = PersistedLogEntries::load(storage, migration_dir).await?;
        let end = persisted.end();
        if !readonly && end.is_some() {
            return Err(Error::NonEmptyWritableLog);
        }

        let immutable = vec![];
        let mutable = LogEntry {
            start: persisted.end().unwrap_or(0),
            ops: vec![],
        };
        let watchers = vec![];
        Ok(Self {
            persisted,
            immutable,
            mutable,
            watchers,
            entry_max_size,
        })
    }

    pub fn write(
        &mut self,
        op: ExecutorOperation,
        log_to_evict_immutable: Arc<Mutex<Log<S>>>,
    ) -> Option<JoinHandle<()>> {
        // Record operation.
        self.mutable.ops.push(op);

        // Check watchers.
        // If watcher.end == self.mutable.end(), send the operations and remove the watcher.
        self.watchers.retain_mut(|watcher| {
            debug_assert!(!watcher.request.is_empty());
            debug_assert!(self.mutable.start <= watcher.request.start);
            if watcher.request.end == self.mutable.end() {
                trigger_watcher(watcher, &self.mutable);
                false
            } else {
                true
            }
        });

        if self.mutable.ops.len() == self.entry_max_size {
            debug!(
                "Log entry has reached max size, start={}, end={}",
                self.mutable.start,
                self.mutable.end()
            );
            // Remove all watchers that wants part of this entry.
            self.watchers.retain_mut(|watcher| {
                if self.mutable.contains(watcher.request.start) {
                    trigger_watcher(watcher, &self.mutable);
                    false
                } else {
                    true
                }
            });

            // Freeze this entry.
            let start = self.mutable.end();
            let mut log_entry = LogEntry { start, ops: vec![] };
            std::mem::swap(&mut self.mutable, &mut log_entry);
            self.immutable.push(log_entry);

            // Persist immutable entries, reload persisted data and evict immutable entries.
            let storage = self.persisted.storage.clone();
            let migration_dir = self.persisted.migration_dir.clone();
            let immutable = self.immutable.clone();
            Some(tokio::spawn(async move {
                for entry in immutable {
                    if let Err(e) = persist(&entry, &storage, &migration_dir).await {
                        error!("Failed to persist log entry: {:?}", e);
                        return;
                    }
                }

                match PersistedLogEntries::load(storage, migration_dir).await {
                    Ok(persisted) => log_to_evict_immutable
                        .lock()
                        .await
                        .evict_immutable_if_possible(persisted),
                    Err(e) => error!("Failed to reload persisted log entries: {:?}", e),
                }
            }))
        } else {
            None
        }
    }

    /// Returned `LogResponse` is guaranteed to contain `request.start`, but may actually contain less then `request.end`.
    pub fn read(&mut self, mut request: Range<usize>) -> LogResponseFuture {
        if request.is_empty() {
            return LogResponseFuture::Ready(vec![]);
        }

        // If start falls in persisted range, return persisted data.
        for persisted in &self.persisted.objects {
            if persisted.range.contains(&request.start) {
                return LogResponseFuture::Persisted(persisted.clone());
            }
        }

        // If start falls in immutable range, return immutable data.
        for immutable in &self.immutable {
            if immutable.contains(request.start) {
                request.end = request.end.min(immutable.end());
                return LogResponseFuture::Ready(immutable.clone_range(&request));
            }
        }

        // If end falls in mutable range, return mutable data.
        debug_assert!(self.mutable.start <= request.start);
        if self.mutable.end() >= request.end {
            return LogResponseFuture::Ready(self.mutable.clone_range(&request));
        }

        // Otherwise add watcher.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.watchers.push(Watcher {
            request,
            sender: Some(sender),
        });
        LogResponseFuture::Watching(receiver)
    }
}

#[derive(Debug, PartialEq)]
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

impl<S: Storage> Log<S> {
    fn evict_immutable_if_possible(&mut self, persisted: PersistedLogEntries<S>) {
        // Find the immutable entry whose end is `persisted.end()`.
        let mut index = None;
        if let Some(end) = persisted.end() {
            for (i, entry) in self.immutable.iter().enumerate() {
                if entry.end() == end {
                    index = Some(i);
                    break;
                }
            }
        } else {
            return;
        }

        if let Some(index) = index {
            debug!("Evicting immutable entries: {:?}", 0..=index);
            self.immutable.drain(0..=index);
            self.persisted = persisted;
        } else {
            error!(
                "Failed to find immutable entry to evict: persisted.end()={:?}",
                persisted.end()
            );
        }
    }
}

fn trigger_watcher(watcher: &mut Watcher, entry: &LogEntry) {
    let sender = watcher.sender.take().expect("watcher already triggered");
    watcher.request.end = watcher.request.end.min(entry.end());
    let result = entry.clone_range(&watcher.request);
    if sender.send(result).is_err() {
        error!("Watcher future dropped before triggering");
    }
}

#[cfg(test)]
mod tests;
