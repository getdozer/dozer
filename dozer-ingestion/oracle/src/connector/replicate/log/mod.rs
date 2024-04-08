use std::time::Instant;
use std::{sync::mpsc::SyncSender, time::Duration};

use dozer_ingestion_connector::dozer_types::log::debug;
use dozer_ingestion_connector::{
    dozer_types::{
        chrono::{DateTime, Utc},
        log::{error, info},
    },
    Ingestor,
};
use oracle::Connection;

use crate::connector::{Error, Scn};

mod listing;
mod merge;
mod redo;

pub type TransactionId = [u8; 8];

#[derive(Debug, Clone)]
/// This is a raw row from V$LOGMNR_CONTENTS
pub struct LogManagerContent {
    pub scn: Scn,
    pub timestamp: DateTime<Utc>,
    pub xid: TransactionId,
    pub pxid: TransactionId,
    pub operation_code: u8,
    pub seg_owner: Option<String>,
    pub table_name: Option<String>,
    pub rbasqn: u32,
    pub sql_redo: Option<String>,
    pub csf: u8,
    pub received: Instant,
}

/// `ingestor` is only used for checking if ingestion has ended so we can break the loop.
pub fn log_miner_loop(
    connection: &Connection,
    start_scn: Scn,
    con_id: Option<u32>,
    poll_interval: Duration,
    fetch_batch_size: u32,
    sender: SyncSender<LogManagerContent>,
    ingestor: &Ingestor,
) {
    log_reader_loop(
        connection,
        start_scn,
        con_id,
        poll_interval,
        redo::LogMiner { fetch_batch_size },
        sender,
        ingestor,
    )
}

fn log_reader_loop(
    connection: &Connection,
    mut start_scn: Scn,
    con_id: Option<u32>,
    poll_interval: Duration,
    reader: impl redo::RedoReader,
    sender: SyncSender<LogManagerContent>,
    ingestor: &Ingestor,
) {
    let mut last_scn = start_scn - 1;

    loop {
        debug!(target: "oracle_replication", "Listing logs starting from SCN {}", start_scn);
        let mut logs = match list_logs(connection, start_scn) {
            Ok(logs) => logs,
            Err(e) => {
                if ingestor.is_closed() {
                    return;
                }
                error!("Error listing logs: {}. Retrying.", e);
                continue;
            }
        };

        if logs.is_empty() {
            if ingestor.is_closed() {
                return;
            }
            info!("No logs found, retrying after {:?}", poll_interval);
            std::thread::sleep(poll_interval);
            continue;
        }

        'replicate_logs: while !logs.is_empty() {
            let log = logs.remove(0);
            debug!(target: "oracle_replication",
                "Reading log {} ({}) ({}, {}), starting from {}",
                log.name, log.sequence, log.first_change, log.next_change, last_scn
            );

            let iterator = {
                match reader.read(connection, &log.name, Some(last_scn), con_id) {
                    Ok(iterator) => iterator,
                    Err(e) => {
                        if ingestor.is_closed() {
                            return;
                        }
                        error!("Error reading log {}: {}. Retrying.", log.name, e);
                        break 'replicate_logs;
                    }
                }
            };

            for content in iterator {
                let content = match content {
                    Ok(content) => content,
                    Err(e) => {
                        if ingestor.is_closed() {
                            return;
                        }
                        error!("Error reading log {}: {}. Retrying.", log.name, e);
                        break 'replicate_logs;
                    }
                };
                last_scn = content.scn;
                if sender.send(content).is_err() {
                    return;
                }
            }

            if logs.is_empty() {
                if ingestor.is_closed() {
                    return;
                }
                debug!(target: "oracle_replication", "Read all logs, retrying after {:?}", poll_interval);
                std::thread::sleep(poll_interval);
            } else {
                // If there are more logs, we need to start from the next log's first change.
                start_scn = log.next_change;
                last_scn = start_scn - 1;
            }
        }
    }
}

fn list_logs(connection: &Connection, start_scn: Scn) -> Result<Vec<listing::ArchivedLog>, Error> {
    let logs = merge::list_and_join_online_log(connection, start_scn)?;
    if !log_contains_scn(logs.first(), start_scn) {
        info!(
            "Online log is empty or doesn't contain start scn {}, listing and merging archived logs",
            start_scn
        );
        merge::list_and_merge_archived_log(connection, start_scn, logs)
    } else {
        Ok(logs)
    }
}

fn log_contains_scn(log: Option<&listing::ArchivedLog>, scn: Scn) -> bool {
    log.map_or(false, |log| {
        log.first_change <= scn && log.next_change > scn
    })
}
