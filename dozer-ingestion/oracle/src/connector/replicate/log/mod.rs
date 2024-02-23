use std::{sync::mpsc::SyncSender, time::Duration};

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
    pub rbablk: u32,
    pub rbabyte: u16,
    pub sql_redo: Option<String>,
    pub csf: u8,
}

/// `ingestor` is only used for checking if ingestion has ended so we can break the loop.
pub fn log_miner_loop(
    connection: &Connection,
    start_scn: Scn,
    con_id: Option<u32>,
    poll_interval: Duration,
    sender: SyncSender<LogManagerContent>,
    ingestor: &Ingestor,
) {
    log_reader_loop(
        connection,
        start_scn,
        con_id,
        poll_interval,
        redo::LogMiner,
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
    #[derive(Debug, Clone, Copy)]
    struct LastRba {
        sqn: u32,
        blk: u32,
        byte: u16,
    }
    let mut last_rba: Option<LastRba> = None;

    loop {
        info!("Listing logs starting from SCN {}", start_scn);
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
            info!(
                "Reading log {} ({}) ({}, {}), starting from {:?}",
                log.name, log.sequence, log.first_change, log.next_change, last_rba
            );

            let iterator = {
                let last_rba = last_rba.and_then(|last_rba| {
                    if log.sequence == last_rba.sqn {
                        Some((last_rba.blk, last_rba.byte))
                    } else {
                        None
                    }
                });
                match reader.read(connection, &log.name, last_rba, con_id) {
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
                last_rba = Some(LastRba {
                    sqn: content.rbasqn,
                    blk: content.rbablk,
                    byte: content.rbabyte,
                });
                if sender.send(content).is_err() {
                    return;
                }
            }

            if logs.is_empty() {
                if ingestor.is_closed() {
                    return;
                }
                info!("Read all logs, retrying after {:?}", poll_interval);
                std::thread::sleep(poll_interval);
            } else {
                // If there are more logs, we need to start from the next log's first change.
                start_scn = log.next_change;
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
