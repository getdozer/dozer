use oracle::Connection;

use crate::connector::{Result, Scn};

/// Given a log file name, a redo reader emits `LogManagerContent` rows
pub trait RedoReader {
    type Iterator<'a>: Iterator<Item = Result<LogMinerContent>>;

    /// Reads the `LogManagerContent` rows that have:
    ///
    /// - scn > last_scn
    fn read<'a>(
        &self,
        connection: &'a Connection,
        log_file_name: &str,
        last_scn: Option<Scn>,
        con_id: Option<u32>,
    ) -> Result<Self::Iterator<'a>>;
}

mod log_miner;

pub(super) use log_miner::{add_logfiles, LogMinerSession};

use super::LogMinerContent;
