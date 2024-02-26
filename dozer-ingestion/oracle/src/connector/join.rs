use std::collections::{HashMap, HashSet};

use super::listing::{Constraint, ConstraintColumn, TableColumn};

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: Option<String>,
    pub nullable: Option<String>,
    pub is_primary_key: bool,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

pub fn join_columns_constraints(
    table_columns: Vec<TableColumn>,
    constraint_columns: Vec<ConstraintColumn>,
    constraints: Vec<Constraint>,
) -> HashMap<(String, String), Vec<Column>> {
    let constraints = constraints.into_iter().collect::<HashSet<_>>();
    let mut all_primary_key_columns = HashSet::<(String, String, String)>::new();
    for constraint_column in constraint_columns {
        let Some(column_name) = constraint_column.column_name else {
            continue;
        };
        let constraint = Constraint {
            owner: Some(constraint_column.owner.clone()),
            constraint_name: Some(constraint_column.constraint_name),
        };
        if constraints.contains(&constraint) {
            all_primary_key_columns.insert((
                constraint_column.owner,
                constraint_column.table_name,
                column_name,
            ));
        }
    }

    let mut table_to_columns = HashMap::<(String, String), Vec<Column>>::new();
    for table_column in table_columns {
        let column_triple = (
            table_column.owner,
            table_column.table_name,
            table_column.column_name,
        );
        let is_primary_key = all_primary_key_columns.contains(&column_triple);
        let column = Column {
            name: column_triple.2,
            data_type: table_column.data_type,
            nullable: table_column.nullable,
            is_primary_key,
            precision: table_column.precision,
            scale: table_column.scale,
        };
        let table_pair = (column_triple.0, column_triple.1);
        table_to_columns.entry(table_pair).or_default().push(column);
    }

    table_to_columns
}
