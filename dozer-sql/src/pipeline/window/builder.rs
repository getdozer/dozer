use dozer_types::{
    chrono::Duration,
    types::{FieldDefinition, Schema},
};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, TableFactor, Value};

use crate::pipeline::{
    errors::{JoinError, PipelineError, WindowError},
    expression::builder::ExpressionBuilder,
    pipeline_builder::from::TableOperator,
};

use super::operator::WindowType;

pub(crate) fn window_from_table_operator(
    operator: TableOperator,
    schema: &Schema,
) -> Result<Option<WindowType>, WindowError> {
    let function_name = string_from_sql_object_name(&operator.name);

    if function_name.to_uppercase() == "TUMBLE" {
        let column_index = get_window_column_index(&operator.args, schema)?;
        let interval_arg = operator
            .args
            .get(2)
            .ok_or(WindowError::WindowMissingIntervalArgument)?;
        let interval = get_window_interval(interval_arg)?;

        Ok(Some(WindowType::Tumble {
            column_index,
            interval,
        }))
    } else if function_name.to_uppercase() == "HOP" {
        let column_index = get_window_column_index(&operator.args, schema)?;
        let hop_arg = operator
            .args
            .get(2)
            .ok_or(WindowError::WindowMissingHopSizeArgument)?;
        let hop_size = get_window_hop(hop_arg)?;
        let interval_arg = operator
            .args
            .get(3)
            .ok_or(WindowError::WindowMissingIntervalArgument)?;
        let interval = get_window_interval(interval_arg)?;

        return Ok(Some(WindowType::Hop {
            column_index,
            hop_size,
            interval,
        }));
    } else {
        return Err(WindowError::UnsupportedRelationFunction(function_name));
    }
}

pub(crate) fn window_from_relation(
    relation: &TableFactor,
    schema: &Schema,
) -> Result<Option<WindowType>, WindowError> {
    match relation {
        TableFactor::Table { name, args, .. } => {
            let function_name = string_from_sql_object_name(name);

            if let Some(args) = args {
                if function_name.to_uppercase() == "TUMBLE" {
                    let column_index = get_window_column_index(args, schema)?;
                    let interval_arg = args
                        .get(2)
                        .ok_or(WindowError::WindowMissingIntervalArgument)?;
                    let interval = get_window_interval(interval_arg)?;

                    Ok(Some(WindowType::Tumble {
                        column_index,
                        interval,
                    }))
                } else if function_name.to_uppercase() == "HOP" {
                    let column_index = get_window_column_index(args, schema)?;
                    let hop_arg = args
                        .get(2)
                        .ok_or(WindowError::WindowMissingHopSizeArgument)?;
                    let hop_size = get_window_hop(hop_arg)?;
                    let interval_arg = args
                        .get(3)
                        .ok_or(WindowError::WindowMissingIntervalArgument)?;
                    let interval = get_window_interval(interval_arg)?;

                    return Ok(Some(WindowType::Hop {
                        column_index,
                        hop_size,
                        interval,
                    }));
                } else {
                    return Err(WindowError::UnsupportedRelationFunction(function_name));
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
        } => Ok(None),
        TableFactor::TableFunction { expr: _, alias: _ } => {
            Err(WindowError::UnsupportedTableFunction)
        }
        TableFactor::UNNEST {
            alias: _,
            array_expr: _,
            with_offset: _,
            with_offset_alias: _,
        } => Err(WindowError::UnsupportedUnnest),
        TableFactor::NestedJoin {
            table_with_joins: _,
            alias: _,
        } => Err(WindowError::UnsupportedNestedJoin),
    }
}

pub(crate) fn window_source_name(operator: &TableOperator) -> Result<String, WindowError> {
    let function_name = string_from_sql_object_name(&operator.name);

    if function_name.to_uppercase() == "TUMBLE" || function_name.to_uppercase() == "HOP" {
        let source_arg = operator
            .args
            .get(0)
            .ok_or(WindowError::WindowMissingSourceArgument)?;
        let source_name = get_window_source_name(source_arg)?;

        Ok(source_name)
    } else {
        Err(WindowError::UnsupportedRelationFunction(function_name))
    }
}

fn get_window_interval(interval_arg: &FunctionArg) -> Result<Duration, WindowError> {
    match interval_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(WindowError::WindowInvalidInterval(column_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
                    let interval: Duration = parse_duration_string(s)
                        .map_err(|_| WindowError::WindowInvalidInterval(s.to_owned()))?;
                    Ok(interval)
                }
                _ => Err(WindowError::WindowInvalidInterval(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(WindowError::WindowInvalidInterval("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(WindowError::WindowInvalidInterval("*".to_string())),
        },
    }
}

fn get_window_hop(hop_arg: &FunctionArg) -> Result<Duration, WindowError> {
    match hop_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(WindowError::WindowInvalidHop(column_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
                    let interval: Duration = parse_duration_string(s)
                        .map_err(|_| WindowError::WindowInvalidHop(s.to_owned()))?;
                    Ok(interval)
                }
                _ => Err(WindowError::WindowInvalidHop(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(WindowError::WindowInvalidHop("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(WindowError::WindowInvalidHop("*".to_string())),
        },
    }
}

fn get_window_source_name(arg: &FunctionArg) -> Result<String, WindowError> {
    match arg {
        FunctionArg::Named { name, arg: _ } => {
            let source_name = ExpressionBuilder::normalize_ident(name);
            Err(WindowError::WindowInvalidSource(source_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let source_name = ExpressionBuilder::normalize_ident(ident);
                    Ok(source_name)
                }
                Expr::CompoundIdentifier(ident) => {
                    let source_name = ExpressionBuilder::fullname_from_ident(ident);
                    Ok(source_name)
                }
                _ => Err(WindowError::WindowInvalidColumn(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(WindowError::WindowInvalidColumn("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(WindowError::WindowInvalidColumn("*".to_string())),
        },
    }
}

fn get_window_column_index(args: &[FunctionArg], schema: &Schema) -> Result<usize, WindowError> {
    let column_arg = args
        .get(1)
        .ok_or(WindowError::WindowMissingColumnArgument)?;
    match column_arg {
        FunctionArg::Named { name, arg: _ } => {
            let column_name = ExpressionBuilder::normalize_ident(name);
            Err(WindowError::WindowInvalidColumn(column_name))
        }
        FunctionArg::Unnamed(arg_expr) => match arg_expr {
            FunctionArgExpr::Expr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let column_name = ExpressionBuilder::normalize_ident(ident);
                    let index = get_field_index(&[ident.clone()], schema)
                        .map_err(|_| WindowError::WindowInvalidColumn(column_name.clone()))?;

                    Ok(index.ok_or(WindowError::WindowInvalidColumn(column_name))?)
                }
                Expr::CompoundIdentifier(ident) => {
                    let column_name = ExpressionBuilder::fullname_from_ident(ident);
                    let index = get_field_index(ident, schema)
                        .map_err(|_| WindowError::WindowInvalidColumn(column_name.clone()))?;

                    Ok(index.ok_or(WindowError::WindowInvalidColumn(column_name))?)
                }
                _ => Err(WindowError::WindowInvalidColumn(expr.to_string())),
            },
            FunctionArgExpr::QualifiedWildcard(_) => {
                Err(WindowError::WindowInvalidColumn("*".to_string()))
            }
            FunctionArgExpr::Wildcard => Err(WindowError::WindowInvalidColumn("*".to_string())),
        },
    }
}

fn parse_duration_string(duration_string: &str) -> Result<Duration, WindowError> {
    let duration_string = duration_string
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let duration_tokens = duration_string.split(' ').collect::<Vec<_>>();
    if duration_tokens.len() != 2 {
        return Err(WindowError::WindowInvalidInterval(duration_string));
    }

    let duration_value = duration_tokens[0]
        .parse::<i64>()
        .map_err(|_| WindowError::WindowInvalidInterval(duration_string.to_owned()))?;

    let duration_unit = duration_tokens[1].to_uppercase();

    match duration_unit.as_str() {
        "MILLISECOND" | "MILLISECONDS" => Ok(Duration::milliseconds(duration_value)),
        "SECOND" | "SECONDS" => Ok(Duration::seconds(duration_value)),
        "MINUTE" | "MINUTES" => Ok(Duration::minutes(duration_value)),
        "HOUR" | "HOURS" => Ok(Duration::hours(duration_value)),
        "DAY" | "DAYS" => Ok(Duration::days(duration_value)),
        _ => Err(WindowError::WindowInvalidInterval(duration_string)),
    }
}

pub fn string_from_sql_object_name(name: &ObjectName) -> String {
    let function_name = name
        .0
        .iter()
        .map(ExpressionBuilder::normalize_ident)
        .collect::<Vec<String>>()
        .join(".");
    function_name
}

pub fn get_field_index(ident: &[Ident], schema: &Schema) -> Result<Option<usize>, PipelineError> {
    let tables_matches = |table_ident: &Ident, fd: &FieldDefinition| -> bool {
        match fd.source.clone() {
            dozer_types::types::SourceDefinition::Table {
                connection: _,
                name,
            } => name == table_ident.value,
            dozer_types::types::SourceDefinition::Alias { name } => name == table_ident.value,
            dozer_types::types::SourceDefinition::Dynamic => false,
        }
    };

    let field_index = match ident.len() {
        1 => {
            let field_index = schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.name == ident[0].value)
                .map(|(idx, fd)| (idx, fd.clone()));
            field_index
        }
        2 => {
            let table_name = ident.first().expect("table_name is expected");
            let field_name = ident.last().expect("field_name is expected");

            let index = schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| tables_matches(table_name, f) && f.name == field_name.value)
                .map(|(idx, fd)| (idx, fd.clone()));
            index
        }
        // 3 => {
        //     let connection_name = comp_ident.get(0).expect("connection_name is expected");
        //     let table_name = comp_ident.get(1).expect("table_name is expected");
        //     let field_name = comp_ident.get(2).expect("field_name is expected");
        // }
        _ => {
            return Err(PipelineError::JoinError(JoinError::NameSpaceTooLong(
                ident
                    .iter()
                    .map(|a| a.value.clone())
                    .collect::<Vec<String>>()
                    .join("."),
            )));
        }
    };
    field_index.map_or(Ok(None), |(i, _fd)| Ok(Some(i)))
}

pub(crate) fn relation_is_a_window(relation: &TableFactor) -> Result<bool, WindowError> {
    match relation {
        TableFactor::Table { name, args, .. } => {
            let function_name = string_from_sql_object_name(name);

            if args.is_some() {
                if function_name.to_uppercase() == "TUMBLE" || function_name.to_uppercase() == "HOP"
                {
                    Ok(true)
                } else {
                    Err(WindowError::UnsupportedRelationFunction(function_name))
                }
            } else {
                // not a function, most probably just a relation name
                Ok(false)
            }
        }
        TableFactor::Derived {
            lateral: _,
            subquery: _,
            alias: _,
        } => Ok(false),
        TableFactor::TableFunction { expr: _, alias: _ } => {
            Err(WindowError::UnsupportedTableFunction)
        }
        TableFactor::UNNEST {
            alias: _,
            array_expr: _,
            with_offset: _,
            with_offset_alias: _,
        } => Err(WindowError::UnsupportedUnnest),
        TableFactor::NestedJoin {
            table_with_joins: _,
            alias: _,
        } => Err(WindowError::UnsupportedNestedJoin),
    }
}
