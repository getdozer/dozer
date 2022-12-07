use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::Schema;
use sqlparser::ast::{TableFactor, TableWithJoins};

use crate::pipeline::{errors::PipelineError, expression::builder::normalize_ident};

use super::{
    join::{JoinOperator, JoinOperatorType, JoinTable},
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
    ) -> Box<dyn Processor> {
        if let Ok(join_tables) = build_join_chain(&self.from) {
            Box::new(ProductProcessor::new(join_tables))
        } else {
            panic!("What to do here?");
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

    let left_join_table = get_join_table(&from.relation)?;
    input_tables.insert(0 as PortHandle, left_join_table);

    for (index, join) in from.joins.iter().enumerate() {
        let right_join_table = get_join_table(&join.relation)?;

        let right_join_operator =
            JoinOperator::new(JoinOperatorType::Inner, (index + 1) as PortHandle, 0, 0);
        input_tables.get_mut(&(index as PortHandle)).unwrap().right = Some(right_join_operator);

        // right_join_table.left = match &join.join_operator {
        //     sqlparser::ast::JoinOperator::Inner(constraint) => {
        //         match constraint {
        //             sqlparser::ast::JoinConstraint::On(expression) => todo!(),
        //             sqlparser::ast::JoinConstraint::Using(expression) => {
        //                 return Err(PipelineError::InvalidQuery(
        //                     "Join Constraint USING is not Supported".to_string(),
        //                 ))
        //             }
        //             sqlparser::ast::JoinConstraint::Natural => {
        //                 return Err(PipelineError::InvalidQuery(
        //                     "Natural Join is not Supported".to_string(),
        //                 ))
        //             }
        //             sqlparser::ast::JoinConstraint::None => {
        //                 return Err(PipelineError::InvalidQuery(
        //                     "Join Constraint without ON is not Supported".to_string(),
        //                 ))
        //             }
        //         };

        //         Some(ReverseJoinOperator::new(
        //             JoinOperatorType::Inner,
        //             (index) as PortHandle,
        //             0,
        //             0,
        //         ))
        //     }
        //     sqlparser::ast::JoinOperator::LeftOuter(constraint) => {
        //         return Err(PipelineError::InvalidQuery(
        //             "Left Outer Join is not Supported".to_string(),
        //         ))
        //     }
        //     sqlparser::ast::JoinOperator::RightOuter(constraint) => {
        //         return Err(PipelineError::InvalidQuery(
        //             "Right Outer Join is not Supported".to_string(),
        //         ))
        //     }
        //     sqlparser::ast::JoinOperator::FullOuter(constraint) => {
        //         return Err(PipelineError::InvalidQuery(
        //             "Full Outer Join is not Supported".to_string(),
        //         ))
        //     }
        //     sqlparser::ast::JoinOperator::CrossJoin => {
        //         return Err(PipelineError::InvalidQuery(
        //             "Cross Join is not Supported".to_string(),
        //         ))
        //     }
        //     sqlparser::ast::JoinOperator::CrossApply => {
        //         return Err(PipelineError::InvalidQuery(
        //             "Cross Apply is not Supported".to_string(),
        //         ))
        //     }
        //     sqlparser::ast::JoinOperator::OuterApply => {
        //         return Err(PipelineError::InvalidQuery(
        //             "Outer Apply is not Supported".to_string(),
        //         ))
        //     }
        // };

        input_tables.insert((index + 1) as PortHandle, right_join_table.clone());
    }

    Ok(input_tables)
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
