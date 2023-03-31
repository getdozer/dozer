use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    path::Path,
};

use dozer_cache::errors::{CacheError, LogError};
use dozer_core::executor::ExecutorOperation;

use dozer_types::{bincode, log::error};
use futures_util::Stream;

pub struct LogReader {
    reader: BufReader<File>,
}

impl LogReader {
    pub fn new(path: &Path) -> Result<Self, CacheError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| CacheError::LogFileNotFound(path.to_path_buf()))?;

        let mut reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Stream for LogReader {
    type Item = ExecutorOperation;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        match read_msg(&mut this.reader) {
            Ok(msg) => std::task::Poll::Ready(Some(msg)),
            Err(e) => {
                error!("Error reading log: {}", e);
                std::task::Poll::Ready(None)
            }
        }
    }
}

fn read_msg(reader: &mut BufReader<File>) -> Result<ExecutorOperation, CacheError> {
    let mut buf = [0; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| CacheError::LogError(LogError::ReadError(e)))?;
    let len = u64::from_le_bytes(buf);

    let buf = read_n(reader, len);
    let msg = bincode::deserialize(&buf).map_err(|e| CacheError::map_deserialization_error(e))?;
    Ok(msg)
}
fn read_n<R>(reader: R, bytes_to_read: u64) -> Vec<u8>
where
    R: Read,
{
    let mut buf = vec![];
    let mut chunk = reader.take(bytes_to_read);

    let n = chunk.read_to_end(&mut buf).expect("Didn't read enough");
    assert_eq!(bytes_to_read as usize, n);
    buf
}
