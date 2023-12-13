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
        DataStorage::Local => Ok((
            Box::new(LocalStorage::new(data_dir).await?),
            String::default(),
        )),
        DataStorage::S3(s3) => Ok((
            Box::new(S3Storage::new(s3.region.as_str().into(), s3.bucket_name).await?),
            data_dir,
        )),
    }
}

/// Returns persisted log entries and keys to remove.
async fn load_persisted_log_entries_impl(
    storage: &dyn Storage,
    prefix: String,
    last_epoch_id: Option<u64>,
) -> Result<(Vec<PersistedLogEntry>, Vec<String>), Error> {
    // Load until `last_epoch_id`.
    let mut result = vec![];
    let mut to_remove = vec![];
    let mut continuation_token = None;
    loop {
        let list_output = storage
            .list_objects(prefix.clone(), continuation_token)
            .await?;

        for output in list_output.objects {
            let name = AsRef::<Utf8Path>::as_ref(&output.key)
                .strip_prefix(&prefix)
                .map_err(|_| Error::UnrecognizedLogEntry(output.key.clone()))?;
            let (epoch_id, range) = parse_log_entry_name(name.as_str())
                .ok_or(Error::UnrecognizedLogEntry(output.key.clone()))?;
            if Some(epoch_id) > last_epoch_id {
                to_remove.push(output.key);
            } else {
                result.push(PersistedLogEntry {
                    key: output.key,
                    epoch_id,
                    range,
                });
            }
        }

        continuation_token = list_output.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    // Check if we have expected epoch id.
    let actual_last_epoch_id = result.last().map(|entry| entry.epoch_id);
    if actual_last_epoch_id != last_epoch_id {
        return Err(Error::EpochIdMismatch {
            expected: last_epoch_id,
            actual: actual_last_epoch_id,
        });
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

    Ok((result, to_remove))
}

pub async fn load_persisted_log_entries(
    storage: &dyn Storage,
    prefix: String,
    last_epoch_id: Option<u64>,
) -> Result<Vec<PersistedLogEntry>, Error> {
    let (entries, _) = load_persisted_log_entries_impl(storage, prefix, last_epoch_id).await?;
    Ok(entries)
}

pub async fn load_persisted_and_remove_spurious_log_entries(
    storage: &dyn Storage,
    prefix: String,
    last_epoch_id: Option<u64>,
) -> Result<Vec<PersistedLogEntry>, Error> {
    let (entries, to_remove) =
        load_persisted_log_entries_impl(storage, prefix, last_epoch_id).await?;

    // Remove extra entries. These are persisted in the middle of a checkpointing, but the checkpointing didn't finish.
    if !to_remove.is_empty() {
        storage.delete_objects(to_remove).await?;
    }

    Ok(entries)
}

pub fn persisted_log_entries_end(persisted: &[PersistedLogEntry]) -> Option<usize> {
    persisted.last().map(|o| o.range.end)
}

pub fn persist(
    queue: &Queue,
    prefix: &str,
    epoch_id: u64,
    range: Range<usize>,
    ops: &[LogOperation],
) -> Result<tokio::sync::oneshot::Receiver<String>, Error> {
    let name = log_entry_name(epoch_id, &range);
    let key = AsRef::<Utf8Path>::as_ref(prefix).join(name).to_string();
    let data = bincode::encode_to_vec(ops, bincode::config::legacy())
        .expect("LogOperation must be serializable");
    queue
        .upload_object(key, data)
        .map_err(|_| Error::PersistingThreadQuit)
}

fn parse_log_entry_name(name: &str) -> Option<(u64, Range<usize>)> {
    let mut split = name.split('-');
    let epoch_id = split.next()?;
    let start = split.next()?;
    let end = split.next()?;
    if split.next().is_some() {
        return None;
    }
    let epoch_id = epoch_id.parse().ok()?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    Some((epoch_id, start..end))
}

fn log_entry_name(epoch_id: u64, range: &Range<usize>) -> String {
    // Format with `u64` max number of digits.
    format!("{:020}-{:020}-{:020}", epoch_id, range.start, range.end)
}
