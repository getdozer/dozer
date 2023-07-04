use std::ops::Range;

use camino::Utf8Path;
use dozer_types::{bincode, epoch::ExecutorOperation, log::debug};

use crate::storage::Storage;

use super::{Error, PersistedLogEntry};

/// Invariant:
/// - objects.is_empty() || objects[0].range.start == 0
/// - objects[i + 1].range.start == objects[i].range.end
pub struct PersistedLogEntries<S: Storage> {
    pub storage: S,
    pub migration_dir: String,
    pub objects: Vec<PersistedLogEntry>,
}

impl<S: Storage> PersistedLogEntries<S> {
    pub async fn load(storage: S, migration_dir: String) -> Result<Self, Error> {
        // Load all objects.
        let mut objects = vec![];
        let mut continuation_token = None;
        loop {
            let list_output = storage
                .list_objects(migration_dir.clone(), continuation_token)
                .await?;

            for output in list_output.objects {
                let name = AsRef::<Utf8Path>::as_ref(&output.key)
                    .strip_prefix(&migration_dir)
                    .map_err(|_| Error::UnrecognizedLogEntry(output.key.clone()))?;
                let range = parse_log_entry_name(name.as_str())
                    .ok_or(Error::UnrecognizedLogEntry(output.key.clone()))?;
                objects.push(PersistedLogEntry {
                    key: output.key,
                    range,
                })
            }

            continuation_token = list_output.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        // Check invariants.
        if let Some(first) = objects.first() {
            if first.range.start != 0 {
                return Err(Error::FirstLogEntryNotStartFromZero(first.clone()));
            }
        }

        for (prev, next) in objects.iter().zip(objects.iter().skip(1)) {
            if prev.range.end != next.range.start {
                return Err(Error::LogEntryNotConsecutive(prev.clone(), next.clone()));
            }
        }

        debug!("Loaded {} log entries", objects.len());

        Ok(Self {
            storage,
            migration_dir,
            objects,
        })
    }

    pub fn end(&self) -> Option<usize> {
        self.objects.last().map(|o| o.range.end)
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub start: usize,
    pub ops: Vec<ExecutorOperation>,
}

impl LogEntry {
    pub fn end(&self) -> usize {
        self.start + self.ops.len()
    }

    pub fn contains(&self, start: usize) -> bool {
        self.start <= start && start < self.end()
    }

    pub fn clone_range(&self, range: &Range<usize>) -> Vec<ExecutorOperation> {
        debug!(
            "Cloning range: {:?}, self.start={}, self.end={}",
            range,
            self.start,
            self.end()
        );
        self.ops[range.start - self.start..range.end - self.start].to_vec()
    }
}

pub async fn persist<S: Storage>(
    entry: &LogEntry,
    storage: &S,
    migration_dir: &str,
) -> Result<(), Error> {
    let name = log_entry_name(&(entry.start..entry.end()));
    let key = AsRef::<Utf8Path>::as_ref(migration_dir)
        .join(name)
        .to_string();
    let data = bincode::serialize(&entry.ops)?;
    storage.put_object(key, data).await.map_err(Into::into)
}

fn parse_log_entry_name(name: &str) -> Option<Range<usize>> {
    let mut split = name.split('-');
    let start = split.next()?;
    let end = split.next()?;
    if split.next().is_some() {
        return None;
    }
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    Some(start..end)
}

fn log_entry_name(range: &Range<usize>) -> String {
    // Format with `u64` max number of digits.
    format!("{:020}-{:020}", range.start, range.end)
}
