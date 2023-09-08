use std::ops::Range;

use camino::Utf8Path;
use dozer_types::{bincode, log::debug, models::app_config::DataStorage};

use crate::storage::{self, LocalStorage, Queue, S3Storage, Storage};

use super::{Error, LogOperation, PersistedLogEntry};

pub async fn create_data_storage(
    storage_config: DataStorage,
    data_dir: String,
) -> Result<(Box<dyn Storage>, String), storage::Error> {
    match storage_config {
        DataStorage::Local(()) => Ok((
            Box::new(LocalStorage::new(data_dir).await?),
            String::default(),
        )),
        DataStorage::S3(s3) => Ok((
            Box::new(S3Storage::new(s3.region.as_str().into(), s3.bucket_name).await?),
            data_dir,
        )),
    }
}

pub async fn load_persisted_log_entries(
    storage: &dyn Storage,
    prefix: String,
    num_entries_to_keep: usize,
) -> Result<Vec<PersistedLogEntry>, Error> {
    // Load first `num_entries_to_keep` objects.
    let mut result = vec![];
    let mut to_remove = vec![];
    let mut continuation_token = None;
    loop {
        let list_output = storage
            .list_objects(prefix.clone(), continuation_token)
            .await?;

        for output in list_output.objects {
            if result.len() < num_entries_to_keep {
                let name = AsRef::<Utf8Path>::as_ref(&output.key)
                    .strip_prefix(&prefix)
                    .map_err(|_| Error::UnrecognizedLogEntry(output.key.clone()))?;
                let range = parse_log_entry_name(name.as_str())
                    .ok_or(Error::UnrecognizedLogEntry(output.key.clone()))?;
                result.push(PersistedLogEntry {
                    key: output.key,
                    range,
                });
            } else {
                to_remove.push(output.key);
            }
        }

        continuation_token = list_output.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    // Check if we have expected number of entries.
    if result.len() != num_entries_to_keep {
        return Err(Error::NotEnoughLogEntries {
            expected: num_entries_to_keep,
            actual: result.len(),
        });
    }

    // Remove extra entries. These are persisted in the middle of a checkpointing, but the checkpointing didn't finish.
    if !to_remove.is_empty() {
        storage.delete_objects(to_remove).await?;
    }

    // Check invariants.
    if let Some(first) = result.first() {
        if first.range.start != 0 {
            return Err(Error::FirstLogEntryNotStartFromZero(first.clone()));
        }
    }

    for (prev, next) in result.iter().zip(result.iter().skip(1)) {
        if prev.range.end != next.range.start {
            return Err(Error::LogEntryNotConsecutive(prev.clone(), next.clone()));
        }
    }

    debug!("Loaded {} log entries", result.len());

    Ok(result)
}

pub fn persisted_log_entries_end(persisted: &[PersistedLogEntry]) -> Option<usize> {
    persisted.last().map(|o| o.range.end)
}

pub fn persist(
    queue: &Queue,
    prefix: &str,
    range: Range<usize>,
    ops: &[LogOperation],
) -> Result<tokio::sync::oneshot::Receiver<String>, Error> {
    let name = log_entry_name(&range);
    let key = AsRef::<Utf8Path>::as_ref(prefix).join(name).to_string();
    let data = bincode::serialize(&ops).expect("LogOperation must be serializable");
    queue
        .upload_object(key, data)
        .map_err(|_| Error::PersistingThreadQuit)
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
