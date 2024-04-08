use std::{env, time::Instant};

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::{error, trace},
};
use oracle::{Connection, ResultSet, RowValue};

use crate::connector::{Error, Scn};

use super::{LogManagerContent, RedoReader};

#[derive(Debug, Clone, Copy)]
pub(crate) struct LogMiner {
    pub fetch_batch_size: u32,
}

#[derive(Debug)]
pub(crate) struct LogMinerIter<'a> {
    result_set: ResultSet<'a, LogManagerContent>,
    connection: &'a Connection,
}

impl<'a> Drop for LogMinerIter<'a> {
    fn drop(&mut self) {
        let sql = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
        trace!(target: "oracle_log_miner","{}", sql);
        if let Err(e) = self.connection.execute(sql, &[]) {
            error!("Failed to end log miner: {}", e);
        }
    }
}

impl<'a> Iterator for LogMinerIter<'a> {
    type Item = Result<LogManagerContent, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.result_set.next().map(|row| row.map_err(Into::into))
    }
}

impl RedoReader for LogMiner {
    type Iterator<'a> = LogMinerIter<'a>;

    fn read<'a>(
        &self,
        connection: &'a Connection,
        log_file_name: &str,
        last_scn: Option<Scn>,
        con_id: Option<u32>,
    ) -> Result<Self::Iterator<'a>, Error> {
        let sql =
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :name, OPTIONS => DBMS_LOGMNR.NEW); END;";
        trace!(target: "oracle_log_miner", "{}, {}", sql, log_file_name);
        connection.execute(sql, &[&str_to_sql!(log_file_name)])?;

        if let Some(last_scn) = last_scn {
            let start_scn = last_scn + 1;
            let sql = "
        BEGIN
            DBMS_LOGMNR.START_LOGMNR(
                STARTSCN => :start_scn,
                OPTIONS =>
                    DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                    DBMS_LOGMNR.NO_ROWID_IN_STMT
            );
        END;";
            trace!(target: "oracle_log_miner", "{}, {}", sql, start_scn);
            connection.execute(sql, &[&start_scn])?;
        } else {
            let sql = "
        BEGIN
            DBMS_LOGMNR.START_LOGMNR(
                OPTIONS =>
                    DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                    DBMS_LOGMNR.NO_ROWID_IN_STMT
            );
        END;";
            trace!(target: "oracle_log_miner", "{}", sql);
            connection.execute(sql, &[])?;
        };
        let stmt = |sql| {
            connection
                .statement(sql)
                .fetch_array_size(self.fetch_batch_size)
                .build()
        };

        let base_sql = "SELECT SCN, TIMESTAMP, XID, PXID, OPERATION_CODE, SEG_OWNER, TABLE_NAME, RBASQN, SQL_REDO, CSF FROM V$LOGMNR_CONTENTS";
        let operation_code_filter = env::var("DOZER_ORACLE_LOG_MINER_OPERATION_CODE_FILTER").ok();
        let con_id_filter = "SRC_CON_ID = :con_id";
        let started = std::time::Instant::now();
        let result_set = match (operation_code_filter, con_id) {
            (Some(operation_code_filter), Some(con_id)) => {
                let sql = format!(
                    "{} WHERE {} AND {}",
                    base_sql, operation_code_filter, con_id_filter
                );
                trace!(target: "oracle_log_miner", "{}, {}", sql, con_id);
                stmt(&sql)?.into_result_set_named(&[("con_id", &con_id)])
            }
            (Some(operation_code_filter), None) => {
                let sql = format!("{} WHERE {}", base_sql, operation_code_filter);
                trace!(target: "oracle_log_miner", "{}", sql);
                stmt(&sql)?.into_result_set(&[])
            }
            (None, Some(con_id)) => {
                let sql = format!("{} WHERE {}", base_sql, con_id_filter);
                trace!(target: "oracle_log_miner", "{}, {}", sql, con_id);
                stmt(&sql)?.into_result_set_named(&[("con_id", &con_id)])
            }
            (None, None) => {
                trace!(target: "oracle_log_miner", "{}", base_sql);
                stmt(base_sql)?.into_result_set(&[])
            }
        }?;

        trace!(target: "oracle_log_miner", "LogMiner read took {:?}", started.elapsed());

        Ok(LogMinerIter {
            result_set,
            connection,
        })
    }
}

impl RowValue for LogManagerContent {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let (
            scn,
            timestamp,
            xid,
            pxid,
            operation_code,
            seg_owner,
            table_name,
            rbasqn,
            sql_redo,
            csf,
        ) = <(
            Scn,
            DateTime<Utc>,
            Vec<u8>,
            Vec<u8>,
            u8,
            Option<String>,
            Option<String>,
            u32,
            Option<String>,
            u8,
        ) as RowValue>::get(row)?;
        Ok(LogManagerContent {
            scn,
            timestamp,
            xid: xid.try_into().expect("xid must be 8 bytes"),
            pxid: pxid.try_into().expect("pxid must be 8 bytes"),
            operation_code,
            seg_owner,
            table_name,
            rbasqn,
            sql_redo,
            csf,
            received: Instant::now(),
        })
    }
}
