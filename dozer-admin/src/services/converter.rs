use crate::server::dozer_admin_grpc::{self};
use dozer_types::types::Schema;
use std::convert::From;

impl From<dozer_orchestrator::TableInfo> for dozer_admin_grpc::TableInfo {
    fn from(t: dozer_orchestrator::TableInfo) -> Self {
        let mut columns: Vec<dozer_admin_grpc::ColumnInfo> = Vec::new();
        if let Some(cols) = t.columns {
            cols.iter().for_each(|c| {
                columns.push(dozer_admin_grpc::ColumnInfo {
                    column_name: c.name.to_owned(),
                    is_nullable: true,
                });
            });
        }

        dozer_admin_grpc::TableInfo {
            table_name: t.name,
            columns,
        }
    }
}
