use dozer_ingestion_connector::dozer_types::log::{debug, warn};
use oracle::Connection;

use crate::connector::{Error, Scn};

#[derive(Debug, Clone)]
pub struct ArchivedLog {
    pub name: String,
    pub sequence: u32,
    pub first_change: Scn,
    pub next_change: Scn,
}

impl ArchivedLog {
    pub fn list(connection: &Connection, start_scn: Scn) -> Result<Vec<ArchivedLog>, Error> {
        let sql = "SELECT NAME, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE# FROM V$ARCHIVED_LOG WHERE NEXT_CHANGE# > :start_scn AND STATUS = 'A' ORDER BY SEQUENCE# ASC";
        debug!("{}, {}", sql, start_scn);
        let rows = connection
            .query_as::<(String, u32, Scn, Scn)>(sql, &[&start_scn])
            .unwrap();

        let mut result = vec![];
        for row in rows {
            let (name, sequence, first_change, next_change) = row?;
            let log = ArchivedLog {
                name,
                sequence,
                first_change,
                next_change,
            };
            if is_continuous(result.last(), &log) {
                result.push(log);
            }
        }

        Ok(result)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Log {
    pub group: u32,
    pub sequence: u32,
    pub first_change: Scn,
    pub next_change: Scn,
}

impl Log {
    pub fn list(connection: &Connection, start_scn: Scn) -> Result<Vec<Log>, Error> {
        let sql = "SELECT GROUP#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE# FROM V$LOG WHERE NEXT_CHANGE# > :start_scn ORDER BY SEQUENCE# ASC";
        debug!("{}, {}", sql, start_scn);
        let rows = connection
            .query_as::<(u32, u32, Scn, Scn)>(sql, &[&start_scn])
            .unwrap();

        let mut result = vec![];
        for row in rows {
            let (group, sequence, first_change, next_change) = row?;
            let log = Log {
                group,
                sequence,
                first_change,
                next_change,
            };
            if is_continuous(result.last(), &log) {
                result.push(log);
            }
        }

        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct LogFile {
    pub group: u32,
    pub member: String,
}

impl LogFile {
    pub fn list(connection: &Connection) -> Result<Vec<LogFile>, Error> {
        let sql = "SELECT GROUP#, MEMBER FROM V$LOGFILE WHERE STATUS IS NULL";
        debug!("{}", sql);
        let rows = connection.query_as::<(u32, String)>(sql, &[]).unwrap();

        let mut result = vec![];
        for row in rows {
            let (group, member) = row?;
            let log_file = LogFile { group, member };
            result.push(log_file);
        }

        Ok(result)
    }
}

pub trait HasLogIdentifier {
    fn sequence(&self) -> u32;
    fn first_change(&self) -> Scn;
    fn next_change(&self) -> Scn;
}

impl HasLogIdentifier for ArchivedLog {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn first_change(&self) -> Scn {
        self.first_change
    }

    fn next_change(&self) -> Scn {
        self.next_change
    }
}

impl HasLogIdentifier for Log {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn first_change(&self) -> Scn {
        self.first_change
    }

    fn next_change(&self) -> Scn {
        self.next_change
    }
}

pub fn is_continuous(
    last_log: Option<&impl HasLogIdentifier>,
    current_log: &impl HasLogIdentifier,
) -> bool {
    let Some(last_log) = last_log else {
        return true;
    };

    let sequence_is_continuous = last_log.sequence() + 1 == current_log.sequence();
    let scn_is_continuous = last_log.next_change() == current_log.first_change();

    if sequence_is_continuous != scn_is_continuous {
        warn!(
            "Log {} has next change {}, but log {} has first change {}",
            last_log.sequence(),
            last_log.next_change(),
            current_log.sequence(),
            current_log.first_change()
        );
    }
    sequence_is_continuous && scn_is_continuous
}
