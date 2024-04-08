use oracle::Connection;

use crate::connector::{Error, Scn};

/// Given a log file name, a redo reader emits `LogManagerContent` rows
pub(crate) trait RedoReader {
    type Iterator<'a>: Iterator<Item = Result<LogManagerContent, Error>>;

    /// Reads the `LogManagerContent` rows that have:
    ///
    /// - scn > last_scn
    fn read<'a>(
        &self,
        connection: &'a Connection,
        log_file_name: &str,
        last_scn: Option<Scn>,
        con_id: Option<u32>,
    ) -> Result<Self::Iterator<'a>, Error>;
}

mod log_miner;

pub(crate) use log_miner::LogMiner;

use super::LogManagerContent;
