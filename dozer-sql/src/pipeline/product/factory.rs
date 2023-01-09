use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::Schema;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Ident, JoinConstraint, TableFactor, TableWithJoins,
};

use crate::pipeline::{
    errors::PipelineError,
    expression::builder::{fullname_from_ident, get_field_index, normalize_ident},
};

use super::{
    join::{JoinOperator, JoinOperatorType, JoinTable},
    processor::ProductProcessor,
};

#[derive(Debug)]
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
            OutputPortType::Stateless,
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
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        match build_join_chain(&self.from, input_schemas) {
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
        TableFactor::Table { name, alias, .. } => {
            if let Some(alias_ident) = alias {
                let alias_name = fullname_from_ident(&[alias_ident.name.clone()]);
                Ok(alias_name)
            } else {
                let input_name = name
                    .0
                    .iter()
                    .map(normalize_ident)
                    .collect::<Vec<String>>()
                    .join(".");

                Ok(input_name)
            }
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
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<HashMap<PortHandle, JoinTable>, PipelineError> {
    let mut input_tables = HashMap::new();

    let port = 0 as PortHandle;
    let input_schema = input_schemas.get(&(port as PortHandle));

    if input_schema.is_none() {
        return Err(PipelineError::InvalidRelation);
    }

    let mut left_join_table = get_join_table(&from.relation, input_schema.unwrap())?;
    input_tables.insert(port, left_join_table.clone());

    for (index, join) in from.joins.iter().enumerate() {
        if let Some(input_schema) = input_schemas.get(&((index + 1) as PortHandle)) {
            let mut right_join_table = get_join_table(&join.relation, input_schema)?;

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
    full_ident.starts_with(&left_join_table.name)
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

fn get_join_table(relation: &TableFactor, schema: &Schema) -> Result<JoinTable, PipelineError> {
    Ok(JoinTable::from(relation, schema))
}

fn append_schema(mut output_schema: Schema, table: &str, current_schema: &Schema) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        let mut name = String::from(table);
        name.push('.');
        name.push_str(&field.name);
        field.name = name;
        output_schema.fields.push(field);
    }

    output_schema
}
