use dozer_types::chrono::Duration;
use sqlparser::ast::{FunctionArg, ObjectName, TableFactor};

use crate::pipeline::{errors::JoinError, expression::builder::ExpressionBuilder};

use super::window::WindowType;

pub(crate) fn window_from_relation(
    relation: &TableFactor,
) -> Result<Option<WindowType>, JoinError> {
    match relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            let function_name = string_from_sql_object_name(name);

            if let Some(args) = args {
                if function_name.to_uppercase() == "TUMBLE" {
                    let column_index = get_tumble_window_column_index(args)?;
                    let interval = get_tumble_window_interval(args)?;

                    return Ok(Some(WindowType::Tumble {
                        column_index,
                        interval,
                    }));
                } else if function_name.to_uppercase() == "HOP" {
                    return Ok(Some(WindowType::Hop {
                        column_index: 0,
                        hop_size: Duration::milliseconds(0),
                        interval: Duration::milliseconds(0),
                    }));
                } else {
                    return Err(JoinError::UnsupportedTableFunction(function_name));
                }
            } else {
                // not a function, most probably just a relation name
                return Ok(None);
            }
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => todo!(),
        TableFactor::TableFunction { expr, alias } => todo!(),
        TableFactor::UNNEST {
            alias,
            array_expr,
            with_offset,
            with_offset_alias,
        } => todo!(),
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => todo!(),
    };
}

fn get_tumble_window_interval(args: &[FunctionArg]) -> Result<usize, JoinError> {
    let interval_arg = args
        .get(2)
        .ok_or(JoinError::WindowMissingIntervalArgument)?;

    match interval_arg {
        FunctionArg::Named { name, arg } => todo!(),
        FunctionArg::Unnamed(arg_expr) => todo!(),
    }
}

fn get_tumble_window_column_index(args: &[FunctionArg]) -> _ {
    let column_arg = args.get(1).ok_or(JoinError::WindowMissingColumnArgument)?;
    match column_arg {
        FunctionArg::Named { name, arg } => todo!(),
        FunctionArg::Unnamed(arg_expr) => todo!(),
    }
}

fn string_from_sql_object_name(name: &ObjectName) -> String {
    let function_name = name
        .0
        .iter()
        .map(ExpressionBuilder::normalize_ident)
        .collect::<Vec<String>>()
        .join(".");
    function_name
}
