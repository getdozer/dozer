use std::path::Path;

use dozer_core::executor::ExecutorOperation;
use dozer_types::types::Schema;
use futures_util::Stream;

pub struct LogReader;

impl LogReader {
    pub fn new(path: &Path) -> (Self, Schema) {
        todo!("Load schema. Start watching log file change.")
    }
}

impl Stream for LogReader {
    type Item = ExecutorOperation;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!("Read next operation from log file.")
    }
}
