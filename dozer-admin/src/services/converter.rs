use crate::server::dozer_admin_grpc::{self};

use std::convert::From;

impl From<dozer_orchestrator::TableInfo> for dozer_admin_grpc::TableInfo {
    fn from(table: dozer_orchestrator::TableInfo) -> Self {
        let mut columns: Vec<dozer_admin_grpc::ColumnInfo> = Vec::new();
        table.column_names.into_iter().for_each(|c| {
            columns.push(dozer_admin_grpc::ColumnInfo {
                column_name: c,
                is_nullable: true,
            });
        });

        dozer_admin_grpc::TableInfo {
            table_name: table.name,
            columns,
        }
    }
}
