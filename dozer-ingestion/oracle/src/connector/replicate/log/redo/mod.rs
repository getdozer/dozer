use oracle::Connection;

use crate::connector::Error;

/// Given a log file name, a redo reader emits `LogManagerContent` rows
pub trait RedoReader {
    type Iterator<'a>: Iterator<Item = Result<LogManagerContent, Error>>;

    /// Reads the `LogManagerContent` rows that have:
    ///
    /// - scn >= start_scn
    /// - rba > last_rba.0 || (rba == last_rba.0 && rbabyte > last_rba.1)
    fn read<'a>(
        &self,
        connection: &'a Connection,
        log_file_name: &str,
        last_rba: Option<(u32, u16)>,
        con_id: Option<u32>,
    ) -> Result<Self::Iterator<'a>, Error>;
}

mod log_miner;

pub use log_miner::LogMiner;

use super::LogManagerContent;
