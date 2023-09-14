pub fn is_network_failure(err: &odbc::DiagnosticRecord) -> bool {
    // Reference for ODBC error codes:
    // https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver16
    let sqlstate = &err.get_raw_state()[0..5];
    matches!(
        sqlstate,
        b"01002" /* Disconnect error */ |
        b"08001" /* Client unable to establish connection */ |
        b"08007" /* Connection failure during transaction */ |
        b"08S01" /* Communication link failure */
    )
}
