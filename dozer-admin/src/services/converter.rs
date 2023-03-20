use dozer_types::grpc_types::admin::{ColumnInfo, TableInfo};

pub fn convert_table(table: dozer_orchestrator::TableInfo) -> TableInfo {
    let mut columns: Vec<ColumnInfo> = Vec::new();
    table.column_names.into_iter().for_each(|c| {
        columns.push(ColumnInfo {
            column_name: c,
            is_nullable: true,
        });
    });

    TableInfo {
        table_name: table.name,
        columns,
    }
}
