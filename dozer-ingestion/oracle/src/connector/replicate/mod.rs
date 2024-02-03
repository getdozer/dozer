use self::listing::ArchivedLog;

use super::Scn;

mod listing;
pub mod log_miner;
pub mod merge;

pub fn log_contains_scn(log: Option<&ArchivedLog>, scn: Scn) -> bool {
    log.map_or(false, |log| {
        log.first_change <= scn && log.next_change > scn
    })
}
