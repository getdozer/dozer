use dozer_types::log::error;

use super::queue::Queue;

#[derive(Debug)]
pub struct Object {
    queue: Queue,
    key: String,
    data: Vec<u8>,
}

impl Object {
    pub fn new(queue: Queue, key: String) -> Result<Self, String> {
        queue.create_upload(key.clone())?;
        Ok(Self {
            queue,
            key,
            data: vec![],
        })
    }

    pub fn write(&mut self, data: &[u8]) -> Result<(), String> {
        self.data.extend_from_slice(data);
        if self.data.len() >= 100 * 1024 * 1024 {
            self.queue
                .upload_chunk(self.key.clone(), std::mem::take(&mut self.data))?;
        }
        Ok(())
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    fn drop(&mut self) -> Result<(), String> {
        if !self.data.is_empty() {
            self.queue
                .upload_chunk(self.key.clone(), std::mem::take(&mut self.data))?;
        }
        self.queue.complete_upload(std::mem::take(&mut self.key))?;
        Ok(())
    }
}

impl Drop for Object {
    fn drop(&mut self) {
        if let Err(e) = self.drop() {
            error!("Failed to complete object {}: {e}", self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::tests::create_storage;

    use super::*;

    #[tokio::test]
    async fn object_write_should_merge_data() {
        let (_temp_dir, storage) = create_storage().await;
        let (queue, join_handle) = Queue::new(dyn_clone::clone_box(&*storage), 1);
        let key = "test";
        let num_bytes = (u16::MAX as usize) * 2;
        // Queue must be used outside tokio context.
        let write_handle = std::thread::spawn(move || {
            let mut object = Object::new(queue, key.to_string()).unwrap();
            for _ in 0..num_bytes {
                object.write(&[0]).unwrap();
            }
        });
        join_handle.await.unwrap();
        write_handle.join().unwrap();
        assert_eq!(
            storage.download_object(key.to_string()).await.unwrap(),
            vec![0; num_bytes]
        );
    }
}
