use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    log::debug,
};
use oracle::Connection;

use crate::connector::{Error, Scn};

#[derive(Debug, Clone)]
pub struct LogManagerContent {
    pub commit_scn: Scn,
    pub commit_timestamp: DateTime<Utc>,
    pub operation_code: u8,
    pub seg_owner: Option<String>,
    pub table_name: Option<String>,
    pub sql_redo: Option<String>,
}

impl LogManagerContent {
    pub fn list(
        connection: &Connection,
        start_scn: Scn,
        con_id: Option<u32>,
    ) -> Result<impl Iterator<Item = Result<LogManagerContent, Error>> + '_, Error> {
        type Row = (
            Scn,
            DateTime<Utc>,
            u8,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let rows = if let Some(con_id) = con_id {
            let sql = "SELECT COMMIT_SCN, COMMIT_TIMESTAMP, OPERATION_CODE, SEG_OWNER, TABLE_NAME, SQL_REDO FROM V$LOGMNR_CONTENTS WHERE COMMIT_SCN >= :start_scn AND SRC_CON_ID = :con_id";
            debug!("{}, {}, {}", sql, start_scn, con_id);
            connection.query_as::<Row>(sql, &[&start_scn, &con_id])
        } else {
            let sql = "SELECT COMMIT_SCN, COMMIT_TIMESTAMP, OPERATION_CODE, SEG_OWNER, TABLE_NAME, SQL_REDO FROM V$LOGMNR_CONTENTS WHERE COMMIT_SCN >= :start_scn";
            debug!("{}, {}", sql, start_scn);
            connection.query_as::<Row>(sql, &[&start_scn])
        }?;

        Ok(rows.into_iter().map(|row| {
            let (commit_scn, commit_timestamp, operation_code, seg_owner, table_name, sql_redo) =
                row?;
            Ok(LogManagerContent {
                commit_scn,
                commit_timestamp,
                operation_code,
                seg_owner,
                table_name,
                sql_redo,
            })
        }))
    }
}
