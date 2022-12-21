use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::Schema;
use sqlparser::ast::{Expr as SqlExpr, TableFactor, TableWithJoins};

use crate::pipeline::{errors::PipelineError, expression::builder::normalize_ident};

use super::{
    join::{JoinOperator, JoinOperatorType, JoinTable, ReverseJoinOperator},
    processor::ProductProcessor,
};

pub struct ProductProcessorFactory {
    from: TableWithJoins,
}

impl ProductProcessorFactory {
    /// Creates a new [`ProductProcessorFactory`].
    pub fn new(from: TableWithJoins) -> Self {
        Self { from }
    }
}

impl ProcessorFactory for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        let input_tables = get_input_tables(&self.from).unwrap();
        input_tables
            .iter()
            .enumerate()
            .map(|(number, _)| number as PortHandle)
            .collect::<Vec<PortHandle>>()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Schema, ExecutionError> {
        let mut output_schema = Schema::empty();

        let input_tables = get_input_tables(&self.from)?;
        for (port, table) in input_tables.iter().enumerate() {
            if let Some(current_schema) = input_schemas.get(&(port as PortHandle)) {
                output_schema = append_schema(output_schema, table, current_schema);
            } else {
                return Err(ExecutionError::InvalidPortHandle(port as PortHandle));
            }
        }

        Ok(output_schema)
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        match build_join_chain(&self.from) {
            Ok(join_tables) => Ok(Box::new(ProductProcessor::new(join_tables))),
            Err(e) => Err(ExecutionError::InternalStringError(e.to_string())),
        }
    }
}

/// Returns a vector of input port handles and relative table name
///
/// # Errors
///
/// This function will return an error if it's not possible to get an input name.
pub fn get_input_tables(from: &TableWithJoins) -> Result<Vec<String>, ExecutionError> {
    let mut input_tables = vec![];

    input_tables.insert(0, get_input_name(&from.relation)?);

    for (index, join) in from.joins.iter().enumerate() {
        let input_name = get_input_name(&join.relation)?;
        input_tables.insert(index + 1, input_name);
    }

    Ok(input_tables)
}

/// Returns the table name
///
/// # Errors
///
/// This function will return an error if the input argument is not a Table.
pub fn get_input_name(relation: &TableFactor) -> Result<String, ExecutionError> {
    match relation {
        TableFactor::Table { name, alias: _, .. } => {
            let input_name = name
                .0
                .iter()
                .map(normalize_ident)
                .collect::<Vec<String>>()
                .join(".");

            Ok(input_name)
        }
        _ => Err(ExecutionError::InternalStringError(
            "Invalid Input table".to_string(),
        )),
    }
}

/// Returns an hashmap with the operations to execute the join.
/// Each entry is linked on the left and/or the right to the other side of the Join operation
///
/// # Errors
///
/// This function will return an error if.
pub fn build_join_chain(
    from: &TableWithJoins,
) -> Result<HashMap<PortHandle, JoinTable>, PipelineError> {
    let mut input_tables = HashMap::new();

    let mut left_join_table = get_join_table(&from.relation)?;
    input_tables.insert(0 as PortHandle, left_join_table.clone());

    for (index, join) in from.joins.iter().enumerate() {
        let mut right_join_table = get_join_table(&join.relation)?;

        let (join_op, reverse_join_op) = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint) => match constraint {
                sqlparser::ast::JoinConstraint::On(expression) => {
                    let (left_keys, right_keys) =
                        parse_join_constraint(expression, &left_join_table, &right_join_table)?;

                    (
                        Some(JoinOperator::new(
                            JoinOperatorType::Inner,
                            (index + 1) as PortHandle,
                            right_keys,
                        )),
                        Some(ReverseJoinOperator::new(
                            JoinOperatorType::Inner,
                            (index) as PortHandle,
                            left_keys,
                        )),
                    )
                }
                _ => {
                    return Err(PipelineError::InvalidQuery(
                        "Unsupported Join constraint".to_string(),
                    ))
                }
            },
            _ => {
                return Err(PipelineError::InvalidQuery(
                    "Unsupported Join type".to_string(),
                ))
            }
        };

        input_tables.get_mut(&(index as PortHandle)).unwrap().right = join_op;

        right_join_table.left = reverse_join_op;

        input_tables.insert((index + 1) as PortHandle, right_join_table.clone());

        left_join_table = input_tables
            .get_mut(&(index as PortHandle))
            .unwrap()
            .clone();
    }

    Ok(input_tables)
}

fn parse_join_constraint(
    expression: &sqlparser::ast::Expr,
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Vec<usize>, Vec<usize>), PipelineError> {
    match expression {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                let (mut left_keys, mut right_keys) =
                    parse_join_constraint(left, left_join_table, right_join_table)?;

                let (mut left_keys_from_right, mut right_keys_from_right) =
                    parse_join_constraint(right, left_join_table, right_join_table)?;
                left_keys.append(&mut left_keys_from_right);
                right_keys.append(&mut right_keys_from_right);

                Ok((left_keys, right_keys))
            }
            sqlparser::ast::BinaryOperator::Eq => { 

                let left_key = match left {
                    SqlExpr::Identifier(ident) => parse_identifier(ident)?,
                    _ => return Err(PipelineError::InvalidQuery(
                        "Unsupported Join constraint".to_string(),
                    )),
                };

                let right_key = match right {
                    SqlExpr::Identifier(ident) => parse_identifier(ident)?,
                    _ => return Err(PipelineError::InvalidQuery(
                        "Unsupported Join constraint".to_string(),
                    )),
                }

                Ok(vec![left_key], vec![right_key])
        },
            _ => Err(PipelineError::InvalidQuery(
                "Unsupported Join constraint".to_string(),
            )),
        },
        _ => Err(PipelineError::InvalidQuery(
            "Unsupported Join constraint".to_string(),
        )),
    }
}

fn parse_identifier(ident: &sqlparser::ast::Ident) -> Result<usize, PipelineError> {
    todo!()
}

// fn get_constraint_keys(expression: SqlExpr) -> Result<Ident, PipelineError> {
//     match expression {
//         SqlExpr::BinaryOp { left, op, right } => match op {
//             BinaryOperator::Eq => {
//                 let left_ident = match *left {
//                     SqlExpr::Identifier(ident) => ident,
//                     _ => {
//                         return Err(PipelineError::InvalidQuery(
//                             "Invalid Join Constraint".to_string(),
//                         ));
//                     }
//                 };
//                 Ok(left_ident)
//             }
//             _ => {
//                 return Err(PipelineError::InvalidQuery(
//                     "Invalid Join Constraint".to_string(),
//                 ));
//             }
//         },
//         _ => {
//             return Err(PipelineError::InvalidQuery(
//                 "Invalid Join Constraint".to_string(),
//             ));
//         }
//     }
// }

fn get_join_table(relation: &TableFactor) -> Result<JoinTable, PipelineError> {
    Ok(JoinTable::from(relation))
}

fn append_schema(mut output_schema: Schema, _table: &str, current_schema: &Schema) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        let mut name = String::from(""); //String::from(table);
                                         //name.push('.');
        name.push_str(&field.name);
        field.name = name;
        output_schema.fields.push(field);
    }

    output_schema
}
