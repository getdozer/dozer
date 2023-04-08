use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    path::Path,
    thread::sleep,
    time::Duration,
};

use super::errors::ReaderError;
use dozer_core::executor::ExecutorOperation;
use dozer_types::{
    bincode,
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    log::trace,
};
use futures_util::{FutureExt, Stream};

pub struct LogReader {
    reader: BufReader<File>,
    name: String,
    pos: u64,
    pb: ProgressBar,
    count: u64,
}
const SLEEP_TIME_MS: u16 = 300;
impl LogReader {
    pub fn new(
        path: &Path,
        name: &str,
        pos: u64,
        multi_pb: Option<MultiProgress>,
    ) -> Result<Self, ReaderError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| ReaderError::LogFileNotFound(path.to_path_buf()))?;

        let mut reader = BufReader::new(file);

        reader
            .seek_relative(pos as i64)
            .map_err(|e| ReaderError::SeekError(name.to_string(), pos, e))?;

        let pb = attach_progress(multi_pb);
        pb.set_message(format!("reader: {}", name));
        Ok(Self {
            reader,
            name: name.to_string(),
            pos,
            pb,
            count: 0,
        })
    }
    async fn next_op(&mut self) -> ExecutorOperation {
        loop {
            let msg = read_msg(&mut self.reader);
            match msg {
                Ok((msg, len)) => {
                    self.pos += len;
                    self.count += 1;
                    self.pb.set_position(self.count);
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
                    sleep(Duration::from_millis(SLEEP_TIME_MS.into()));
                }
            }
        }
    }
}

impl Stream for LogReader {
    type Item = ExecutorOperation;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Box::pin(this.next_op()).poll_unpin(cx) {
            std::task::Poll::Ready(msg) => std::task::Poll::Ready(Some(msg)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

fn read_msg(reader: &mut BufReader<File>) -> Result<(ExecutorOperation, u64), ReaderError> {
    let mut buf = [0; 8];
    reader
        .read_exact(&mut buf)
        .map_err(ReaderError::ReadError)?;
    let len = u64::from_le_bytes(buf);

    let buf = read_n(reader, len);
    let msg = bincode::deserialize(&buf).map_err(ReaderError::DeserializationError)?;
    Ok((msg, len + 8))
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

fn attach_progress(multi_pb: Option<MultiProgress>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    multi_pb.as_ref().map(|m| m.add(pb.clone()));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}
