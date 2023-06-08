use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
};

use dozer_types::{
    bytes::{BufMut, BytesMut},
    epoch::ExecutorOperation,
    indicatif::{MultiProgress, ProgressBar},
};

use crate::{attach_progress, errors::WriterError};

#[derive(Debug)]
pub struct LogWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    pb: ProgressBar,
    counter: u64,
}

impl LogWriter {
    pub fn new(
        path: PathBuf,
        file_buffer_capacity: usize,
        name: String,
        multi_pb: Option<MultiProgress>,
    ) -> Result<Self, WriterError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| WriterError::FileSystem(path.clone(), e))?;

        let writer = BufWriter::with_capacity(file_buffer_capacity, file);

        let pb = attach_progress(multi_pb);
        pb.set_message(name);

        Ok(Self {
            writer,
            path,
            pb,
            counter: 0,
        })
    }

    pub fn write_op(&mut self, op: &ExecutorOperation) -> Result<(), WriterError> {
        let msg = dozer_types::bincode::serialize(op)?;

        let mut buf = BytesMut::with_capacity(msg.len() + 4);
        buf.put_u64_le(msg.len() as u64);
        buf.put_slice(&msg);

        self.writer
            .write_all(&buf)
            .map_err(|e| WriterError::FileSystem(self.path.clone(), e))?;

        self.counter += 1;
        self.pb.set_position(self.counter);

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), WriterError> {
        self.writer
            .flush()
            .map_err(|e| WriterError::FileSystem(self.path.clone(), e))
    }
}
