use std::{io::SeekFrom, path::Path, time::Duration};

use dozer_cache::errors::{CacheError, LogError};
use dozer_core::executor::ExecutorOperation;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::{bincode, log::trace};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

pub struct LogReader {
    reader: BufReader<File>,
    name: String,
    pos: u64,
    count: u64,
}
const SLEEP_TIME_MS: u16 = 300;
impl LogReader {
    pub async fn new(path: &Path, name: &str, pos: u64) -> Result<Self, CacheError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .map_err(|_| CacheError::LogFileNotFound(path.to_path_buf()))?;

        let mut reader = BufReader::new(file);

        reader
            .seek(SeekFrom::Start(pos))
            .await
            .map_err(|e| CacheError::LogError(LogError::SeekError(name.to_string(), pos, e)))?;

        let pb = attach_progress(multi_pb);
        pb.set_message(format!("reader: {}", name));

        Ok(Self {
            reader,
            name: name.to_string(),
            pos,
            count: 0,
        })
    }

    pub async fn next_op(&mut self) -> ExecutorOperation {
        loop {
            let msg = read_msg(&mut self.reader).await;
            match msg {
                Ok((msg, len)) => {
                    self.pos += len;
                    self.count += 1;
                    return msg;
                }
                Err(e) => {
                    trace!(
                        "Error reading log : {}, Going to sleep : {} ms, Error : {:?}",
                        self.name,
                        SLEEP_TIME_MS,
                        e
                    );

                    //  go to sleep for a bit
                    tokio::time::sleep(Duration::from_millis(SLEEP_TIME_MS.into())).await;
                }
            }
        }
    }
}

async fn read_msg(reader: &mut BufReader<File>) -> Result<(ExecutorOperation, u64), LogError> {
    let mut buf = [0; 8];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(LogError::ReadError)?;
    let len = u64::from_le_bytes(buf);

    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(LogError::ReadError)?;
    let msg = bincode::deserialize(&buf).map_err(LogError::DeserializationError)?;
    Ok((msg, len + 8))
}
