use std::collections::HashMap;

use oracle::Connection;

use crate::connector::{Error, Scn};

use super::listing::{is_continuous, ArchivedLog, Log, LogFile};

pub fn list_and_join_online_log(
    connection: &Connection,
    start_scn: Scn,
) -> Result<Vec<ArchivedLog>, Error> {
    let logs = Log::list(connection, start_scn)?;
    let log_files = LogFile::list(connection)?;
    let mut log_files = log_files
        .into_iter()
        .map(|log_file| (log_file.group, log_file.member))
        .collect::<HashMap<_, _>>();

    let mut result = vec![];
    for log in logs {
        if let Some(name) = log_files.remove(&log.group) {
            let archived_log = ArchivedLog {
                name,
                sequence: log.sequence,
                first_change: log.first_change,
                next_change: log.next_change,
            };
            result.push(archived_log);
        } else {
            // We only want continuous logs
            break;
        }
    }

    Ok(result)
}

pub fn list_and_merge_archived_log(
    connection: &Connection,
    start_scn: Scn,
    mut online_logs: Vec<ArchivedLog>,
) -> Result<Vec<ArchivedLog>, Error> {
    let mut archived_logs = ArchivedLog::list(connection, start_scn)?;
    let first_continuous_online_log_index = online_logs
        .iter()
        .position(|log| is_continuous(archived_logs.last(), log));
    if let Some(index) = first_continuous_online_log_index {
        archived_logs.extend(online_logs.drain(index..));
    }
    Ok(archived_logs)
}
