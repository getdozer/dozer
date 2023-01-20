use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::Schema;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Ident, JoinConstraint};

use crate::pipeline::{
    errors::{PipelineError, UnsupportedSqlError},
    expression::builder::{fullname_from_ident, get_field_index},
    new_builder::{get_input_names, IndexedTabelWithJoins, NameOrAlias},
};

use super::{
    join::{JoinOperator, JoinOperatorType, JoinTable},
    processor::ProductProcessor,
};

#[derive(Debug)]
pub struct ProductProcessorFactory {
    input_tables: IndexedTabelWithJoins,
}

impl ProductProcessorFactory {
    /// Creates a new [`ProductProcessorFactory`].
    pub fn new(input_tables: IndexedTabelWithJoins) -> Self {
        Self { input_tables }
    }
}

impl ProcessorFactory for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        let input_names = get_input_names(&self.input_tables);
        input_names
            .iter()
            .enumerate()
            .map(|(number, _)| number as PortHandle)
            .collect::<Vec<PortHandle>>()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Schema, ExecutionError> {
        let mut output_schema = Schema::empty();
        let input_names = get_input_names(&self.input_tables);
        for (port, table) in input_names.iter().enumerate() {
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
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        match build_join_chain(&self.input_tables, input_schemas) {
            Ok(join_tables) => Ok(Box::new(ProductProcessor::new(join_tables))),
            Err(e) => Err(ExecutionError::InternalStringError(e.to_string())),
        }
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

/// Returns an hashmap with the operations to execute the join.
/// Each entry is linked on the left and/or the right to the other side of the Join operation
///
/// # Errors
///
/// This function will return an error if.
pub fn build_join_chain(
    join_tables: &IndexedTabelWithJoins,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<HashMap<PortHandle, JoinTable>, PipelineError> {
    let mut input_tables = HashMap::new();

    let port = 0 as PortHandle;
    let input_schema = input_schemas.get(&(port as PortHandle));

    if input_schema.is_none() {
        return Err(PipelineError::InvalidRelation);
    }

    let mut left_join_table = JoinTable::from(&join_tables.relation.0, input_schema.unwrap());
    input_tables.insert(port, left_join_table.clone());

    for (index, (relation_name, join)) in join_tables.joins.iter().enumerate() {
        if let Some(input_schema) = input_schemas.get(&((index + 1) as PortHandle)) {
            let mut right_join_table = JoinTable::from(&relation_name, input_schema);

            let join_op = match &join.join_operator {
                sqlparser::ast::JoinOperator::Inner(constraint) => match constraint {
                    JoinConstraint::On(expression) => {
                        let (left_keys, right_keys) =
                            parse_join_constraint(expression, &left_join_table, &right_join_table)?;

                        JoinOperator::new(
                            JoinOperatorType::Inner,
                            (index + 1) as PortHandle,
                            left_keys,
                            (index) as PortHandle,
                            right_keys,
                        )
                    }
                    _ => {
                        return Err(PipelineError::UnsupportedSqlError(
                            UnsupportedSqlError::UnsupportedJoinConstraint,
                        ))
                    }
                },
                _ => {
                    return Err(PipelineError::UnsupportedSqlError(
                        UnsupportedSqlError::UnsupportedJoinType,
                    ))
                }
            };

            input_tables.get_mut(&(index as PortHandle)).unwrap().right = Some(join_op.clone());

            right_join_table.left = Some(join_op);

            input_tables.insert((index + 1) as PortHandle, right_join_table.clone());

            left_join_table = input_tables
                .get_mut(&((index + 1) as PortHandle))
                .unwrap()
                .clone();
        } else {
            return Err(PipelineError::InvalidRelation);
        }
    }

    Ok(input_tables)
}

fn parse_join_constraint(
    expression: &sqlparser::ast::Expr,
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Vec<usize>, Vec<usize>), PipelineError> {
    match expression {
        SqlExpr::BinaryOp {
            ref left,
            op,
            ref right,
        } => match op {
            BinaryOperator::And => {
                let (mut left_keys, mut right_keys) =
                    parse_join_constraint(left, left_join_table, right_join_table)?;

                let (mut left_keys_from_right, mut right_keys_from_right) =
                    parse_join_constraint(right, left_join_table, right_join_table)?;
                left_keys.append(&mut left_keys_from_right);
                right_keys.append(&mut right_keys_from_right);

                Ok((left_keys, right_keys))
            }
            BinaryOperator::Eq => {
                let mut left_key_indexes = vec![];
                let mut right_key_indexes = vec![];
                match *left.clone() {
                    SqlExpr::Identifier(ident) => {
                        let (left_key_index, right_key_index) =
                            parse_compound_identifier(&[ident], left_join_table, right_join_table)?;
                        if let Some(left_index) = left_key_index {
                            left_key_indexes.push(left_index);
                        } else if let Some(right_index) = right_key_index {
                            right_key_indexes.push(right_index);
                        } else {
                            return Err(PipelineError::InvalidQuery(
                                "Invalid Join constraint".to_string(),
                            ));
                        }
                    }
                    SqlExpr::CompoundIdentifier(ident) => {
                        let (left_key_index, right_key_index) =
                            parse_compound_identifier(&ident, left_join_table, right_join_table)?;
                        if let Some(left_index) = left_key_index {
                            left_key_indexes.push(left_index);
                        } else if let Some(right_index) = right_key_index {
                            right_key_indexes.push(right_index);
                        } else {
                            return Err(PipelineError::InvalidQuery(
                                "Invalid Join constraint".to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(PipelineError::InvalidQuery(
                            "Unsupported Join constraint".to_string(),
                        ))
                    }
                }

                match *right.clone() {
                    SqlExpr::Identifier(ident) => {
                        let (left_key_index, right_key_index) =
                            parse_compound_identifier(&[ident], left_join_table, right_join_table)?;
                        if let Some(left_index) = left_key_index {
                            left_key_indexes.push(left_index);
                        } else if let Some(right_index) = right_key_index {
                            right_key_indexes.push(right_index);
                        } else {
                            return Err(PipelineError::InvalidQuery(
                                "Invalid Join constraint".to_string(),
                            ));
                        }
                    }
                    SqlExpr::CompoundIdentifier(ident) => {
                        let (left_key_index, right_key_index) =
                            parse_compound_identifier(&ident, left_join_table, right_join_table)?;

                        if let Some(left_index) = left_key_index {
                            left_key_indexes.push(left_index);
                        } else if let Some(right_index) = right_key_index {
                            right_key_indexes.push(right_index);
                        } else {
                            return Err(PipelineError::InvalidQuery(
                                "Invalid Join constraint".to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(PipelineError::InvalidQuery(
                            "Unsupported Join constraint".to_string(),
                        ))
                    }
                }

                Ok((left_key_indexes, right_key_indexes))
            }
            _ => Err(PipelineError::InvalidQuery(
                "Unsupported Join constraint".to_string(),
            )),
        },
        _ => Err(PipelineError::InvalidQuery(
            "Unsupported Join constraint".to_string(),
        )),
    }
}

fn from_table(ident: &[Ident], left_join_table: &JoinTable) -> bool {
    let full_ident = fullname_from_ident(ident);
    full_ident.starts_with(&left_join_table.name.0)
        || left_join_table
            .name
            .1
            .as_ref()
            .map_or(false, |a| full_ident.starts_with(a))
}

fn parse_compound_identifier(
    ident: &[Ident],
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Option<usize>, Option<usize>), PipelineError> {
    if from_table(ident, left_join_table) {
        Ok((Some(get_field_index(ident, &left_join_table.schema)?), None))
    } else if from_table(ident, right_join_table) {
        Ok((
            None,
            Some(get_field_index(ident, &right_join_table.schema)?),
        ))
    } else {
        Err(PipelineError::InvalidExpression(
            "Invalid Field in the Join Constraint".to_string(),
        ))
    }
}

fn append_schema(
    mut output_schema: Schema,
    table: &NameOrAlias,
    current_schema: &Schema,
) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        let mut name = table.1.as_ref().map_or(table.0.clone(), |a| a.to_string());
        name.push('.');
        name.push_str(&field.name);
        field.name = name;
        output_schema.fields.push(field);
    }
    output_schema.print().printstd();
    output_schema
}
