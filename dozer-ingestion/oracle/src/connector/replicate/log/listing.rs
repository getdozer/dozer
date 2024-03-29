use std::{collections::HashSet, time::Duration};

use dozer_ingestion_connector::dozer_types::log::debug;
use oracle::{
    sql_type::{FromSql, OracleType, ToSql},
    Connection, RowValue, Statement,
};

use crate::connector::{Error, Result, Scn};

pub(super) struct LogCollector<'a> {
    connection: &'a Connection,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum LogType {
    Online = 0,
    Archived = 1,
}

impl FromSql for LogType {
    fn from_sql(val: &oracle::SqlValue) -> oracle::Result<Self>
    where
        Self: Sized,
    {
        let v: u8 = val.get()?;
        Ok(v.into())
    }
}

impl ToSql for LogType {
    fn oratype(&self, _conn: &Connection) -> oracle::Result<oracle::sql_type::OracleType> {
        Ok(OracleType::Number(1, 0))
    }

    fn to_sql(&self, val: &mut oracle::SqlValue) -> oracle::Result<()> {
        val.set(&(*self as u8))
    }
}

impl From<u8> for LogType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Online,
            1 => Self::Archived,
            _ => unreachable!("Invalid value for `LogType`: {value}"),
        }
    }
}

impl<'a> LogCollector<'a> {
    pub(super) fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }
}

#[derive(Debug)]
pub(super) struct Logs {
    logs: Vec<ArchivedLog>,
    sequences: HashSet<u32>,
}

impl<'a> IntoIterator for &'a Logs {
    type Item = &'a ArchivedLog;

    type IntoIter = std::slice::Iter<'a, ArchivedLog>;

    fn into_iter(self) -> Self::IntoIter {
        self.logs.iter()
    }
}

impl PartialEq for Logs {
    fn eq(&self, other: &Self) -> bool {
        self.sequences == other.sequences
    }
}

impl Logs {
    fn new(logs: Vec<ArchivedLog>) -> Self {
        let sequences: HashSet<u32> = logs.iter().map(|log| log.sequence).collect();
        // No duplicates allowed
        assert_eq!(sequences.len(), logs.len());
        Self { logs, sequences }
    }
}

impl LogCollector<'_> {
    fn get_logs_stmt(&self, start_scn: Scn) -> Result<Statement> {
        let online_sql = r#"
        select 
            MIN(F.MEMBER)       as NAME,
            LOG.FIRST_CHANGE#   as FIRST_CHANGE,
            LOG.NEXT_CHANGE#    as NEXT_CHANGE,
            :online_type        as TYPE,
            LOG.SEQUENCE#       as SEQUENCE
        from
            V$LOG LOG
        inner join V$LOGFILE F 
            on LOG.GROUP# = F.GROUP#
        left join V$ARCHIVED_LOG ALOG 
            on 
                ALOG.FIRST_CHANGE# = LOG.FIRST_CHANGE#
                and
                ALOG.NEXT_CHANGE# = LOG.NEXT_CHANGE#
        where
            LOG.STATUS != 'UNUSED'
            and
            (LOG.STATUS = 'CURRENT' or LOG.NEXT_CHANGE# >= :online_scn)
            and
            -- Exclude online logs that are also archived and get them
            -- from the archive instead
            (ALOG.STATUS <> 'A' OR ALOG.FIRST_CHANGE# IS NULL)
        group by
            LOG.GROUP#, LOG.FIRST_CHANGE#, LOG.NEXT_CHANGE#, LOG.STATUS, 
            LOG.ARCHIVED, LOG.SEQUENCE#
        "#;

        let archived_sql = r"
        select 
            NAME            as NAME,
            FIRST_CHANGE#   as FIRST_CHANGE,
            NEXT_CHANGE#    as NEXT_CHANGE,
            :archive_type   as TYPE,
            SEQUENCE#       as SEQUENCE
        from
            V$ARCHIVED_LOG
        where
            NAME is not null
            and
            STATUS = 'A'
            and
            NEXT_CHANGE# > :archive_scn
            -- and
            -- If the log is archived to multiple destinations, just take the 
            -- first valid local one, to prevent duplicates
            -- DEST_ID in (select DEST_ID from V$ARCHIVE_DEST_STATUS where STATUS = 'VALID' and TYPE = 'LOCAL' AND ROWNUM = 1)
        ";

        let sql = format!(
            r"
        {online_sql} 
        union 
        {archived_sql} 
        order by SEQUENCE"
        );

        let mut stmt = self.connection.statement(&sql).build()?;
        stmt.bind("online_scn", &start_scn)?;
        stmt.bind("archive_scn", &start_scn)?;
        stmt.bind("online_type", &LogType::Online)?;
        stmt.bind("archive_type", &LogType::Archived)?;
        Ok(stmt)
    }

    fn get_logs_inner(&self, start_scn: Scn) -> Result<Vec<ArchivedLog>> {
        let mut stmt = self.get_logs_stmt(start_scn)?;
        let values = stmt
            .query_as::<ArchivedLog>(&[])?
            .map(|v| v.map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        let (mut online, mut archive) = values
            .into_iter()
            .partition::<Vec<_>, _>(|value| value.r#type == LogType::Online);
        for online_log in &online {
            archive.retain(|archive_log| archive_log.sequence != online_log.sequence);
        }
        online.append(&mut archive);
        online.sort_unstable_by_key(|log| log.sequence);
        // This is basically slice::array_windows::<2>(), but that is not stable
        // yet
        /*
                for i in 0..online.len() - 1 {
                    if online[i].sequence != online[i + 1].sequence {
                        online.truncate(i+1);
                        break;
                    }
                }
        */
        Ok(online)
    }

    pub(super) fn get_logs(&self, start_scn: Scn) -> Result<Logs> {
        let mut delay = Duration::from_millis(10);
        let max_time = Duration::from_secs(60);
        let mut total = Duration::ZERO;
        while total < max_time {
            let logs = self.get_logs_inner(start_scn);
            match logs {
                Ok(logs) if !logs.is_empty() => {
                    debug!("Found {} logs for SCN {start_scn}", logs.len());
                    return Ok(Logs::new(logs));
                }
                Ok(_) => {
                    debug!("No logs available for SCN {start_scn}. Retrying..");
                    std::thread::sleep(delay);
                    total += delay;
                    delay *= 2;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::NoLogsFound(start_scn))
    }
}

#[derive(Debug, Clone, RowValue)]
pub(super) struct ArchivedLog {
    pub(super) name: String,
    pub(super) sequence: u32,
    #[row_value(rename = "type")]
    pub(super) r#type: LogType,
}
