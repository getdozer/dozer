use dozer_types::{chrono::Duration, types::Schema};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, ObjectName, TableFactor, Value};

use crate::pipeline::{errors::JoinError, expression::builder::ExpressionBuilder};

use super::{factory::get_field_index, window::WindowType};

pub(crate) fn window_from_relation(
    relation: &TableFactor,
    schema: &Schema,
) -> Result<Option<WindowType>, JoinError> {
    match relation {
        TableFactor::Table {
            name,
            alias: _,
            args,
            with_hints: _,
        } => {
            let function_name = string_from_sql_object_name(name);

            if let Some(args) = args {
                if function_name.to_uppercase() == "TUMBLE" {
                    let column_index = get_window_column_index(args, schema)?;
                    let interval_arg = args
                        .get(2)
                        .ok_or(JoinError::WindowMissingIntervalArgument)?;
                    let interval = get_window_interval(interval_arg)?;

                    Ok(Some(WindowType::Tumble {
                        column_index,
                        interval,
                    }))
                } else if function_name.to_uppercase() == "HOP" {
                    let column_index = get_window_column_index(args, schema)?;
                    let hop_arg = args.get(2).ok_or(JoinError::WindowMissingHopSizeArgument)?;
                    let hop_size = get_window_hop(hop_arg)?;
                    let interval_arg = args
                        .get(3)
                        .ok_or(JoinError::WindowMissingIntervalArgument)?;
                    let interval = get_window_interval(interval_arg)?;

                    return Ok(Some(WindowType::Hop {
                        column_index,
                        hop_size,
                        interval,
                    }));
                } else {
                    return Err(JoinError::UnsupportedRelationFunction(function_name));
                }
            } else {
                // not a function, most probably just a relation name
                Ok(None)
            }
        }
        TableFactor::Derived {
            lateral: _,
            subquery: _,
            alias: _,
        } => Err(JoinError::UnsupportedDerivedTable),
        TableFactor::TableFunction { expr: _, alias: _ } => {
            Err(JoinError::UnsupportedTableFunction)
        }
        TableFactor::UNNEST {
            alias: _,
            array_expr: _,
            with_offset: _,
            with_offset_alias: _,
        } => Err(JoinError::UnsupportedUnnest),
        TableFactor::NestedJoin {
            table_with_joins: _,
            alias: _,
        } => Err(JoinError::UnsupportedNestedJoin),
    }
}

fn get_window_interval(interval_arg: &FunctionArg) -> Result<Duration, JoinError> {
    match interval_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(JoinError::WindowInvalidInterval(column_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
                    let interval: Duration = parse_duration_string(s)
                        .map_err(|_| JoinError::WindowInvalidInterval(s.to_owned()))?;
                    Ok(interval)
                }
                _ => Err(JoinError::WindowInvalidInterval(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(JoinError::WindowInvalidInterval("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(JoinError::WindowInvalidInterval("*".to_string())),
        },
    }
}

fn get_window_hop(hop_arg: &FunctionArg) -> Result<Duration, JoinError> {
    match hop_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(JoinError::WindowInvalidHop(column_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
                    let interval: Duration = parse_duration_string(s)
                        .map_err(|_| JoinError::WindowInvalidHop(s.to_owned()))?;
                    Ok(interval)
                }
                _ => Err(JoinError::WindowInvalidHop(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(JoinError::WindowInvalidHop("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(JoinError::WindowInvalidHop("*".to_string())),
        },
    }
}

fn get_window_column_index(args: &[FunctionArg], schema: &Schema) -> Result<usize, JoinError> {
    let column_arg = args.get(1).ok_or(JoinError::WindowMissingColumnArgument)?;
    match column_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(JoinError::WindowInvalidColumn(column_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let column_name = ExpressionBuilder::normalize_ident(ident);
                    let index = get_field_index(&[ident.clone()], schema)
                        .map_err(|_| JoinError::WindowInvalidColumn(column_name.clone()))?;

                    Ok(index.ok_or(JoinError::WindowInvalidColumn(column_name))?)
                }
                Expr::CompoundIdentifier(ident) => {
                    let column_name = ExpressionBuilder::fullname_from_ident(ident);
                    let index = get_field_index(ident, schema)
                        .map_err(|_| JoinError::WindowInvalidColumn(column_name.clone()))?;

                    Ok(index.ok_or(JoinError::WindowInvalidColumn(column_name))?)
                }
                _ => Err(JoinError::WindowInvalidColumn(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(JoinError::WindowInvalidColumn("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(JoinError::WindowInvalidColumn("*".to_string())),
        },
    }
}

fn parse_duration_string(duration_string: &str) -> Result<Duration, JoinError> {
    let duration_string = duration_string
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let duration_tokens = duration_string.split(' ').collect::<Vec<_>>();
    if duration_tokens.len() != 2 {
        return Err(JoinError::WindowInvalidInterval(duration_string));
    }

    let duration_value = duration_tokens[0]
        .parse::<i64>()
        .map_err(|_| JoinError::WindowInvalidInterval(duration_string.to_owned()))?;

    let duration_unit = duration_tokens[1].to_uppercase();

    match duration_unit.as_str() {
        "MILLISECONDS" => Ok(Duration::milliseconds(duration_value)),
        "SECONDS" => Ok(Duration::seconds(duration_value)),
        "MINUTES" => Ok(Duration::minutes(duration_value)),
        "HOURS" => Ok(Duration::hours(duration_value)),
        "DAYS" => Ok(Duration::days(duration_value)),
        _ => Err(JoinError::WindowInvalidInterval(duration_string)),
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
