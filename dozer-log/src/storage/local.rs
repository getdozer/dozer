use std::{
    collections::{hash_map::Entry, HashMap},
    num::NonZeroU16,
    sync::Arc,
};

use camino::Utf8Path;
use dozer_types::{
    bytes::Bytes,
    grpc_types::internal::{self, storage_response},
    tonic::async_trait,
    tracing::debug,
};
use futures_util::{stream::BoxStream, StreamExt};
use tempdir::TempDir;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    sync::Mutex,
};
use tokio_util::io::ReaderStream;

use super::{Error, ListObjectsOutput, ListedObject, Storage};

#[derive(Debug, Clone)]
pub struct LocalStorage {
    root: String,
    /// Map from key to upload id, from upload id to the temp dir that stores the parts.
    multipart_uploads: Arc<Mutex<HashMap<String, HashMap<String, TempDir>>>>,
}

impl LocalStorage {
    pub async fn new(root: String) -> Result<Self, Error> {
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|e| Error::FileSystem(root.clone(), e))?;
        Ok(Self {
            root,
            multipart_uploads: Default::default(),
        })
    }

    async fn get_path(&self, key: &str) -> Result<String, Error> {
        let path = AsRef::<Utf8Path>::as_ref(&self.root).join(key).to_string();
        if let Some(parent) = AsRef::<Utf8Path>::as_ref(&path).parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::FileSystem(parent.to_string(), e))?;
        }
        Ok(path)
    }
}

#[async_trait]
impl Storage for LocalStorage {
    fn describe(&self) -> storage_response::Storage {
        storage_response::Storage::Local(internal::LocalStorage {
            root: self.root.clone(),
        })
    }

    async fn put_object(&self, key: String, data: Vec<u8>) -> Result<(), Error> {
        let path = self.get_path(&key).await?;
        debug!("putting object to {}", path);
        write(path, &data)
    }

    async fn create_multipart_upload(&self, key: String) -> Result<String, Error> {
        let temp_dir =
            TempDir::new("local-storage").map_err(|e| Error::TempDir(key.to_string(), e))?;
        let path = temp_dir.path();
        let upload_id = path
            .to_str()
            .ok_or(Error::NonUtf8Path(path.to_path_buf()))?
            .to_string();
        let mut multipart_uploads = self.multipart_uploads.lock().await;
        match multipart_uploads.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(upload_id.clone(), temp_dir);
            }
            Entry::Vacant(entry) => {
                entry.insert([(upload_id.clone(), temp_dir)].into_iter().collect());
            }
        }
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        key: String,
        upload_id: String,
        part_number: NonZeroU16,
        data: Vec<u8>,
    ) -> Result<String, Error> {
        self.multipart_uploads
            .lock()
            .await
            .get(&key)
            .and_then(|uploads| uploads.get(&upload_id))
            .ok_or(Error::UploadNotFound {
                key: key.clone(),
                upload_id: upload_id.clone(),
            })?;
        let part_path = get_path_path(&upload_id, part_number);
        write(part_path.clone(), &data)?;
        Ok(part_path)
    }

    async fn complete_multipart_upload(
        &self,
        key: String,
        upload_id: String,
        mut parts: Vec<(NonZeroU16, String)>,
    ) -> Result<(), Error> {
        parts.sort();

        let path = self.get_path(&key).await?;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| Error::FileSystem(path.clone(), e))?;
        for (_, file_name) in parts {
            let data = read(file_name).await?;
            file.write_all(&data)
                .await
                .map_err(|e| Error::FileSystem(path.clone(), e))?;
        }

        self.multipart_uploads
            .lock()
            .await
            .get_mut(&key)
            .and_then(|uploads| uploads.remove(&upload_id))
            .ok_or(Error::UploadNotFound {
                key: key.clone(),
                upload_id: upload_id.clone(),
            })
            .map(|_| ())
    }

    async fn list_objects(
        &self,
        prefix: String,
        _continuation_token: Option<String>,
    ) -> Result<ListObjectsOutput, Error> {
        // Traverse the root directory and find all files with the given prefix.
        let mut objects = vec![];
        list_objects_recursive(&self.root, self.root.clone(), &prefix, &mut objects)?;
        objects.sort();
        Ok(ListObjectsOutput {
            objects,
            continuation_token: None,
        })
    }

    async fn get_object(
        &self,
        key: String,
    ) -> Result<BoxStream<Result<Bytes, std::io::Error>>, Error> {
        let path = self.get_path(&key).await?;
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| Error::FileSystem(path, e))?;
        let file = BufReader::new(file);
        Ok(ReaderStream::new(file).boxed())
    }

    async fn delete_objects(&self, keys: Vec<String>) -> Result<(), Error> {
        for key in keys {
            let path = self.get_path(&key).await?;
            std::fs::remove_file(&path).map_err(|e| Error::FileSystem(path, e))?;
        }
        Ok(())
    }
}

fn write(path: String, contents: &[u8]) -> Result<(), Error> {
    std::fs::write(&path, contents).map_err(|e| Error::FileSystem(path, e))
}

async fn read(path: String) -> Result<Vec<u8>, Error> {
    tokio::fs::read(&path)
        .await
        .map_err(|e| Error::FileSystem(path, e))
}

fn get_path_path(upload_id: &str, part_number: NonZeroU16) -> String {
    AsRef::<Utf8Path>::as_ref(upload_id)
        .join(part_number.to_string())
        .to_string()
}

fn list_objects_recursive(
    root: &str,
    current: String,
    prefix: &str,
    objects: &mut Vec<ListedObject>,
) -> Result<(), Error> {
    for entry in AsRef::<Utf8Path>::as_ref(&current)
        .read_dir_utf8()
        .map_err(|e| Error::FileSystem(current.clone(), e))?
    {
        let entry = entry.map_err(|e| Error::FileSystem(current.clone(), e))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|e| Error::FileSystem(path.to_string(), e))?;
        if metadata.is_file() {
            let key = path
                .strip_prefix(root)
                .expect("we're traversing root")
                .to_string();
            if key.starts_with(prefix) {
                let last_modified = metadata
                    .modified()
                    .map_err(|e| Error::FileSystem(path.to_string(), e))?;
                objects.push(ListedObject { key, last_modified })
            }
        } else if metadata.is_dir() {
            list_objects_recursive(root, path.to_string(), prefix, objects)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_storage_basic() {
        let temp_dir = TempDir::new("test_local_storage_basic").unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        super::super::tests::test_storage_basic(&storage).await;
    }

    #[tokio::test]
    async fn test_local_storage_multipart() {
        let temp_dir = TempDir::new("test_local_storage_multipart").unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        super::super::tests::test_storage_multipart(&storage).await;
    }

    #[tokio::test]
    async fn test_local_storage_prefix() {
        let temp_dir = TempDir::new("test_local_storage_prefix").unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        super::super::tests::test_storage_prefix(&storage).await;
    }

    #[tokio::test]
    async fn test_local_storage_empty_multipart() {
        let temp_dir = TempDir::new("test_local_storage_empty_multipart").unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        super::super::tests::test_storage_empty_multipart(&storage).await;
    }
}
