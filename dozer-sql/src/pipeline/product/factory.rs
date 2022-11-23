use std::collections::HashMap;

use dozer_core::{
    dag::{
        executor_local::DEFAULT_PORT_HANDLE,
        node::{
            PortHandle, StatefulPortHandle, StatefulPortHandleOptions, StatefulProcessor,
            StatefulProcessorFactory,
        },
    },
    storage::common::Environment,
};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Ident, TableFactor, TableWithJoins};

use crate::pipeline::{
    errors::PipelineError,
    expression::{builder::normalize_ident, execution::Expression},
};

use super::{
    join::{JoinOperator, JoinOperatorType, JoinTable, ReverseJoinOperator},
    processor::ProductProcessor,
};

use crate::pipeline::product::factory::PipelineError::InvalidRelation;

pub struct ProductProcessorFactory {
    from: TableWithJoins,
}

impl ProductProcessorFactory {
    /// Creates a new [`ProductProcessorFactory`].
    pub fn new(from: TableWithJoins) -> Self {
        Self { from }
    }
}

impl StatefulProcessorFactory for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        let input_tables = get_input_tables(&self.from).unwrap();
        input_tables
            .iter()
            .enumerate()
            .map(|(number, _)| number as PortHandle)
            .collect::<Vec<PortHandle>>()
    }

    fn get_output_ports(&self) -> Vec<StatefulPortHandle> {
        vec![StatefulPortHandle::new(
            DEFAULT_PORT_HANDLE,
            StatefulPortHandleOptions::default(),
        )]
    }

    fn build(&self) -> Box<dyn StatefulProcessor> {
        Box::new(ProductProcessor::new(self.from.clone()))
    }
}

/// Returns a vector of input port handles and relative table name
///
/// # Errors
///
/// This function will return an error if it's not possible to get an input name.
pub fn get_input_tables(from: &TableWithJoins) -> Result<Vec<String>, PipelineError> {
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
pub fn get_input_name(relation: &TableFactor) -> Result<String, PipelineError> {
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
        _ => Err(InvalidRelation),
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

    let left_join_table = get_join_table(&from.relation)?;
    input_tables.insert(0 as PortHandle, left_join_table);

    for (index, join) in from.joins.iter().enumerate() {
        let mut right_join_table = get_join_table(&join.relation)?;

        let right_join_operator =
            JoinOperator::new(JoinOperatorType::Inner, (index + 1) as PortHandle, 0, 0);
        input_tables.get_mut(&(index as PortHandle)).unwrap().right = Some(right_join_operator);

        right_join_table.left = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint) => {
                match constraint {
                    sqlparser::ast::JoinConstraint::On(expression) => todo!(),
                    sqlparser::ast::JoinConstraint::Using(expression) => {
                        return Err(PipelineError::InvalidQuery(
                            "Join Constraint USING is not Supported".to_string(),
                        ))
                    }
                    sqlparser::ast::JoinConstraint::Natural => {
                        return Err(PipelineError::InvalidQuery(
                            "Natural Join is not Supported".to_string(),
                        ))
                    }
                    sqlparser::ast::JoinConstraint::None => {
                        return Err(PipelineError::InvalidQuery(
                            "Join Constraint without ON is not Supported".to_string(),
                        ))
                    }
                };

                Some(ReverseJoinOperator::new(
                    JoinOperatorType::Inner,
                    (index) as PortHandle,
                    0,
                    0,
                ))
            }
            sqlparser::ast::JoinOperator::LeftOuter(constraint) => {
                return Err(PipelineError::InvalidQuery(
                    "Left Outer Join is not Supported".to_string(),
                ))
            }
            sqlparser::ast::JoinOperator::RightOuter(constraint) => {
                return Err(PipelineError::InvalidQuery(
                    "Right Outer Join is not Supported".to_string(),
                ))
            }
            sqlparser::ast::JoinOperator::FullOuter(constraint) => {
                return Err(PipelineError::InvalidQuery(
                    "Full Outer Join is not Supported".to_string(),
                ))
            }
            sqlparser::ast::JoinOperator::CrossJoin => {
                return Err(PipelineError::InvalidQuery(
                    "Cross Join is not Supported".to_string(),
                ))
            }
            sqlparser::ast::JoinOperator::CrossApply => {
                return Err(PipelineError::InvalidQuery(
                    "Cross Apply is not Supported".to_string(),
                ))
            }
            sqlparser::ast::JoinOperator::OuterApply => {
                return Err(PipelineError::InvalidQuery(
                    "Outer Apply is not Supported".to_string(),
                ))
            }
        };

        input_tables.insert((index + 1) as PortHandle, right_join_table.clone());
    }

    Ok(input_tables)
}

fn get_constraint_keys(expression: SqlExpr) -> Result<Ident, PipelineError> {
    match expression {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let left_ident = match *left {
                    SqlExpr::Identifier(ident) => ident,
                    _ => {
                        return Err(PipelineError::InvalidQuery(
                            "Invalid Join Constraint".to_string(),
                        ));
                    }
                };
                Ok(left_ident)
            }
            _ => {
                return Err(PipelineError::InvalidQuery(
                    "Invalid Join Constraint".to_string(),
                ));
            }
        },
        _ => {
            return Err(PipelineError::InvalidQuery(
                "Invalid Join Constraint".to_string(),
            ));
        }
    }
}

fn get_join_table(relation: &TableFactor) -> Result<JoinTable, PipelineError> {
    Ok(JoinTable::from(relation))
}
