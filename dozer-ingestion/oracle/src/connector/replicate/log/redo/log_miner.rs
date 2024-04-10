use oracle::{Connection, Statement};
use dozer_ingestion_connector::dozer_types::log::debug;
use dozer_ingestion_connector::dozer_types::tracing::field::debug;

use crate::connector::{replicate::log::listing::Logs, Result, Scn};

pub(crate) struct LogMinerSession<'a> {
    fetch_batch_size: u32,
    start_scn: Scn,
    end_scn: Scn,
    connection: &'a Connection,
}

pub(crate) struct AddedLogfiles;

pub(crate) fn add_logfiles(connection: &Connection, files: &Logs) -> Result<AddedLogfiles> {
    let sql =
        "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :name, OPTIONS => DBMS_LOGMNR.ADDFILE); END;";
    let mut batch = connection.batch(sql, 100).build()?;
    for file in files {
        batch.append_row(&[&file.name])?;
    }
    batch.execute()?;
    Ok(AddedLogfiles)
}
impl<'a> LogMinerSession<'a> {
    pub(crate) fn start(
        connection: &'a Connection,
        start: Scn,
        end: Scn,
        fetch_batch_size: u32,
        _files: &AddedLogfiles,
    ) -> Result<Self> {
        let sql = "
        BEGIN
            DBMS_LOGMNR.START_LOGMNR(
                STARTSCN => :startscn,
                ENDSCN => :endscn,
                OPTIONS =>
                    DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                    DBMS_LOGMNR.NO_ROWID_IN_STMT
            );
        END;";
        connection.execute_named(sql, &[("startscn", &start), ("endscn", &end)])?;
        Ok(Self {
            start_scn: start,
            end_scn: end,
            connection,
            fetch_batch_size,
        })
    }

    pub(crate) fn stmt(&self, con_id: Option<u32>) -> Result<Statement> {
        log_miner_stmt(
            self.connection,
            con_id,
            self.start_scn,
            self.end_scn,
            self.fetch_batch_size,
        )
    }

    pub(crate) fn end(self, _files: AddedLogfiles) -> Result<()> {
        let sql = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
        self.connection.execute(sql, &[])?;
        Ok(())
    }
}

fn log_miner_stmt(
    connection: &Connection,
    con_id: Option<u32>,
    start_scn: Scn,
    end_scn: Scn,
    fetch_batch_size: u32,
) -> Result<Statement> {
    let mut sql = "SELECT SCN, TIMESTAMP, XID, PXID, OPERATION_CODE, SEG_OWNER, TABLE_NAME, SQL_REDO, CSF FROM V$LOGMNR_CONTENTS WHERE SCN >= :start_scn AND SCN < :end_scn".to_owned();

    if con_id.is_some() {
        sql += " AND SRC_CON_ID = :con_id";
    }

    let mut stmt = connection
        .statement(&sql)
        .prefetch_rows(fetch_batch_size)
        .fetch_array_size(fetch_batch_size)
        .build()?;

    debug!(target: "oracle_log_miner", "{sql} {start_scn} -> {end_scn}");

    stmt.bind("start_scn", &start_scn)?;
    stmt.bind("end_scn", &end_scn)?;

    if let Some(con_id) = con_id {
        stmt.bind("con_id", &con_id)?;
    }
    Ok(stmt)
}
