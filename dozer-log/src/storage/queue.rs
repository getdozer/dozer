use std::{
    collections::{hash_map::Entry, HashMap},
    num::NonZeroU16,
};

use dozer_types::{
    log::{debug, error},
    thiserror::{self, Error},
};
use nonzero_ext::nonzero;
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

use super::Storage;

#[derive(Debug, Clone)]
pub struct Queue {
    sender: Sender<Request>,
}

impl Queue {
    pub fn new(storage: Box<dyn Storage>, capacity: usize) -> (Self, JoinHandle<()>) {
        let (sender, requests) = mpsc::channel(capacity);
        let worker = tokio::spawn(upload_loop(storage, requests));
        (Self { sender }, worker)
    }

    pub fn create_upload(&self, key: String) -> Result<oneshot::Receiver<String>, String> {
        self.send_request(key, RequestKind::CreateUpload)
    }

    pub fn upload_chunk(
        &self,
        key: String,
        data: Vec<u8>,
    ) -> Result<oneshot::Receiver<String>, String> {
        self.send_request(key, RequestKind::UploadChunk(data))
    }

    pub fn complete_upload(&self, key: String) -> Result<oneshot::Receiver<String>, String> {
        self.send_request(key, RequestKind::CompleteUpload)
    }

    pub fn upload_object(
        &self,
        key: String,
        data: Vec<u8>,
    ) -> Result<oneshot::Receiver<String>, String> {
        self.send_request(key, RequestKind::UploadObject(data))
    }

    fn send_request(
        &self,
        key: String,
        kind: RequestKind,
    ) -> Result<oneshot::Receiver<String>, String> {
        let (return_sender, return_receiver) = oneshot::channel();
        self.sender
            .blocking_send(Request {
                key,
                kind,
                return_sender,
            })
            .map_err(|e| e.0.key)?;
        Ok(return_receiver)
    }
}

#[derive(Debug)]
struct Request {
    key: String,
    kind: RequestKind,
    return_sender: oneshot::Sender<String>,
}

#[derive(Debug, Clone)]
enum RequestKind {
    CreateUpload,
    UploadChunk(Vec<u8>),
    CompleteUpload,
    UploadObject(Vec<u8>),
}

struct MultipartUpload {
    id: String,
    parts: Vec<(NonZeroU16, String)>,
}

async fn upload_loop(storage: Box<dyn Storage>, mut requests: Receiver<Request>) {
    let mut multipart_uploads = HashMap::new();

    while let Some(request) = requests.recv().await {
        loop {
            match handle_request(
                &*storage,
                &mut multipart_uploads,
                &request.key,
                request.kind.clone(),
            )
            .await
            {
                Ok(()) => {
                    if let Err(key) = request.return_sender.send(request.key) {
                        debug!("No one is waiting for the uploading result of {}", key);
                    }
                    break;
                }
                Err(e) => {
                    error!("error uploading {}: {e}", request.key);
                }
            }
        }
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] super::Error),
    #[error("upload already exists")]
    UploadAlreadyExists,
    #[error("upload not found")]
    UploadNotFound,
    #[error("too many parts")]
    TooManyParts,
}

async fn handle_request(
    storage: &dyn Storage,
    multipart_uploads: &mut HashMap<String, MultipartUpload>,
    key: &str,
    request: RequestKind,
) -> Result<(), Error> {
    match request {
        RequestKind::CreateUpload => match multipart_uploads.entry(key.to_string()) {
            Entry::Vacant(entry) => {
                let upload_id = storage.create_multipart_upload(entry.key().clone()).await?;
                entry.insert(MultipartUpload {
                    id: upload_id,
                    parts: vec![],
                });
            }
            Entry::Occupied(_) => {
                return Err(Error::UploadAlreadyExists);
            }
        },
        RequestKind::UploadChunk(data) => {
            let upload = multipart_uploads
                .get_mut(key)
                .ok_or(Error::UploadNotFound)?;
            let part_number = match upload.parts.last() {
                Some((last_part_number, _)) => {
                    last_part_number.checked_add(1).ok_or(Error::TooManyParts)?
                }
                None => nonzero!(1u16),
            };
            let part_id = storage
                .upload_part(key.to_string(), upload.id.clone(), part_number, data)
                .await?;
            upload.parts.push((part_number, part_id));
        }
        RequestKind::CompleteUpload => {
            let (key, upload) = multipart_uploads
                .remove_entry(key)
                .ok_or(Error::UploadNotFound)?;
            storage
                .complete_multipart_upload(key, upload.id, upload.parts)
                .await?;
        }
        RequestKind::UploadObject(data) => {
            storage.put_object(key.to_string(), data).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::storage::create_temp_dir_local_storage;

    use super::*;

    #[tokio::test]
    async fn test_handle_request() {
        let (_temp_dir, storage) = create_temp_dir_local_storage().await;
        let mut multipart_uploads = HashMap::new();
        let key = "test";
        let data = vec![1, 2, 3];
        handle_request(
            &*storage,
            &mut multipart_uploads,
            key,
            RequestKind::CreateUpload,
        )
        .await
        .unwrap();
        handle_request(
            &*storage,
            &mut multipart_uploads,
            key,
            RequestKind::UploadChunk(data.clone()),
        )
        .await
        .unwrap();
        handle_request(
            &*storage,
            &mut multipart_uploads,
            key,
            RequestKind::CompleteUpload,
        )
        .await
        .unwrap();
        assert_eq!(
            storage.download_object(key.to_string()).await.unwrap(),
            data
        );
        assert!(multipart_uploads.is_empty());
    }

    #[tokio::test]
    async fn test_handle_request_upload_already_exists() {
        let (_temp_dir, storage) = create_temp_dir_local_storage().await;
        let mut multipart_uploads = HashMap::new();
        let key = "test";
        handle_request(
            &*storage,
            &mut multipart_uploads,
            key,
            RequestKind::CreateUpload,
        )
        .await
        .unwrap();
        let error = handle_request(
            &*storage,
            &mut multipart_uploads,
            key,
            RequestKind::CreateUpload,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::UploadAlreadyExists));
    }

    #[tokio::test]
    async fn test_handle_request_upload_not_found() {
        let (_temp_dir, storage) = create_temp_dir_local_storage().await;
        let mut multipart_uploads = HashMap::new();
        let key = "test";
        let error = handle_request(
            &*storage,
            &mut multipart_uploads,
            key,
            RequestKind::UploadChunk(vec![]),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::UploadNotFound));
    }
}
