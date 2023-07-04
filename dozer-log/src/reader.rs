use std::{io::SeekFrom, time::Duration};

use crate::attach_progress;

use super::errors::ReaderError;
use camino::Utf8Path;
use dozer_types::epoch::ExecutorOperation;
use dozer_types::indicatif::{MultiProgress, ProgressBar};
use dozer_types::{bincode, log::trace};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

pub struct LogReader {
    reader: BufReader<File>,
    name: String,
    pos: u64,
    pb: ProgressBar,
    count: u64,
}
const SLEEP_TIME_MS: u16 = 300;
impl LogReader {
    pub async fn new(
        path: &Utf8Path,
        name: String,
        pos: u64,
        multi_pb: Option<MultiProgress>,
    ) -> Result<Self, ReaderError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .map_err(|_| ReaderError::LogFileNotFound(path.to_string().into()))?;

        let mut reader = BufReader::new(file);

        reader
            .seek(SeekFrom::Start(pos))
            .await
            .map_err(|e| ReaderError::SeekError(name.clone(), pos, e))?;

        let pb = attach_progress(multi_pb);
        pb.set_message(format!("reader: {}", name));

        Ok(Self {
            reader,
            name,
            pos,
            pb,
            count: 0,
        })
    }

    pub async fn next_op(&mut self) -> (ExecutorOperation, u64) {
        loop {
            let msg = read_msg(&mut self.reader).await;
            match msg {
                Ok((msg, len)) => {
                    self.pos += len;
                    self.count += 1;
                    self.pb.set_position(self.count);
                    return (msg, self.pos);
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

async fn read_msg(reader: &mut BufReader<File>) -> Result<(ExecutorOperation, u64), ReaderError> {
    let mut buf = [0; 8];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(ReaderError::ReadError)?;
    let len = u64::from_le_bytes(buf);

    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(ReaderError::ReadError)?;
    let msg = bincode::deserialize(&buf).map_err(ReaderError::DeserializationError)?;
    Ok((msg, len + 8))
}
