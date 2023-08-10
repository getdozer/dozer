use std::{ops::Range, time::Duration};

use camino::Utf8Path;
use dozer_types::{
    bincode,
    log::{debug, error},
    models::app_config::DataStorage,
};
use tokio::task::JoinHandle;

use crate::storage::{self, LocalStorage, S3Storage, Storage};

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
) -> Result<Vec<PersistedLogEntry>, Error> {
    // Load all objects.
    let mut result = vec![];
    let mut continuation_token = None;
    loop {
        let list_output = storage
            .list_objects(prefix.clone(), continuation_token)
            .await?;

        for output in list_output.objects {
            let name = AsRef::<Utf8Path>::as_ref(&output.key)
                .strip_prefix(&prefix)
                .map_err(|_| Error::UnrecognizedLogEntry(output.key.clone()))?;
            let range = parse_log_entry_name(name.as_str())
                .ok_or(Error::UnrecognizedLogEntry(output.key.clone()))?;
            result.push(PersistedLogEntry {
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

#[derive(Debug)]
struct PersistRequest {
    ops: Vec<LogOperation>,
    range: Range<usize>,
    return_key: tokio::sync::oneshot::Sender<String>,
}

#[derive(Debug)]
pub struct PersistingQueue {
    request_sender: tokio::sync::mpsc::Sender<PersistRequest>,
    worker: Option<JoinHandle<()>>,
}

const RETRY_INTERVAL: Duration = Duration::from_secs(1);

impl PersistingQueue {
    pub async fn new(
        storage: Box<dyn Storage>,
        prefix: String,
        max_num_immutable_entries: usize,
    ) -> Result<Self, Error> {
        let (request_sender, mut request_receiver) =
            tokio::sync::mpsc::channel::<PersistRequest>(max_num_immutable_entries);
        let worker = tokio::spawn(async move {
            while let Some(mut request) = request_receiver.recv().await {
                let name = log_entry_name(&request.range);
                let key = AsRef::<Utf8Path>::as_ref(prefix.as_str())
                    .join(&name)
                    .to_string();
                let data = loop {
                    match bincode::serialize(&request.ops) {
                        Ok(data) => {
                            request.ops.clear(); // To save some memory
                            break data;
                        }
                        Err(e) => {
                            error!("Failed to serialize log entry: {e}, retrying in {RETRY_INTERVAL:?}");
                            tokio::time::sleep(RETRY_INTERVAL).await;
                        }
                    }
                };
                loop {
                    match storage.put_object(key.clone(), data.clone()).await {
                        Ok(()) => {
                            if request.return_key.send(key).is_err() {
                                debug!("No one wants to know that we persisted {name}");
                            }
                            break;
                        }
                        Err(e) => {
                            error!(
                                "Failed to persist log entry: {e}, retrying in {RETRY_INTERVAL:?}"
                            );
                            tokio::time::sleep(RETRY_INTERVAL).await;
                        }
                    }
                }
            }
        });
        Ok(Self {
            request_sender,
            worker: Some(worker),
        })
    }

    pub async fn persist(
        &mut self,
        range: Range<usize>,
        ops: Vec<LogOperation>,
    ) -> Result<tokio::sync::oneshot::Receiver<String>, Error> {
        let (return_key, receiver) = tokio::sync::oneshot::channel();
        if self
            .request_sender
            .send(PersistRequest {
                ops,
                range,
                return_key,
            })
            .await
            .is_err()
        {
            let worker = self
                .worker
                .take()
                .ok_or(Error::PersistingThreadQuit(None))?;
            // Worker has quit. It must have panicked.
            let Err(e) = worker.await else {
                panic!("Worker must have panicked");
            };
            return Err(Error::PersistingThreadQuit(Some(e)));
        }
        Ok(receiver)
    }
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
