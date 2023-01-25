use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::{Schema, SourceDefinition};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, JoinConstraint};

use crate::pipeline::expression::builder::ConstraintIdentifier;
use crate::pipeline::product::from_factory::get_field_index;
use crate::pipeline::{
    builder::SchemaSQLContext,
    errors::{JoinError, PipelineError},
    expression::builder::extend_schema_source_def,
};
use crate::pipeline::{
    builder::{get_input_names, IndexedTabelWithJoins},
    expression::builder::NameOrAlias,
};
use sqlparser::ast::Expr;

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

impl ProcessorFactory<SchemaSQLContext> for ProductProcessorFactory {
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
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let mut output_schema = Schema::empty();
        let input_names = get_input_names(&self.input_tables);
        for (port, table) in input_names.iter().enumerate() {
            if let Some((current_schema, _)) = input_schemas.get(&(port as PortHandle)) {
                output_schema = append_schema(output_schema, table, current_schema);
            } else {
                return Err(ExecutionError::InvalidPortHandle(port as PortHandle));
            }
        }

        Ok((output_schema, SchemaSQLContext::default()))
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
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
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

    let port = 0;
    let input_schema = input_schemas.get(&(port as PortHandle)).map_or(
        Err(JoinError::InvalidJoinConstraint(
            join_tables.relation.0.clone().0,
        )),
        Ok,
    )?;

    let input_schema = extend_schema_source_def(input_schema, &join_tables.relation.0);

    let mut left_join_table = JoinTable::from(&input_schema);
    input_tables.insert(port as PortHandle, left_join_table.clone());

    for (index, (relation_name, join)) in join_tables.joins.iter().enumerate() {
        let input_schema = input_schemas.get(&((index + 1) as PortHandle)).map_or(
            Err(JoinError::InvalidJoinConstraint(
                join_tables.relation.0.clone().0,
            )),
            Ok,
        )?;
        let input_schema = extend_schema_source_def(input_schema, relation_name);
        let mut right_join_table = JoinTable::from(&input_schema);

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
                    return Err(PipelineError::JoinError(
                        JoinError::UnsupportedJoinConstraint,
                    ))
                }
            },
            _ => return Err(PipelineError::JoinError(JoinError::UnsupportedJoinType)),
        };

        input_tables.get_mut(&(index as PortHandle)).unwrap().right = Some(join_op.clone());

        right_join_table.left = Some(join_op);

        input_tables.insert((index + 1) as PortHandle, right_join_table.clone());

        left_join_table = input_tables
            .get_mut(&((index + 1) as PortHandle))
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

                let (left_arr, right_arr) =
                    parse_join_eq_expression(left, left_join_table, right_join_table)?;
                left_key_indexes.extend(left_arr);
                right_key_indexes.extend(right_arr);

                let (left_arr, right_arr) =
                    parse_join_eq_expression(right, left_join_table, right_join_table)?;
                left_key_indexes.extend(left_arr);
                right_key_indexes.extend(right_arr);

                Ok((left_key_indexes, right_key_indexes))
            }
            _ => Err(PipelineError::JoinError(
                JoinError::UnsupportedJoinConstraint,
            )),
        },
        _ => Err(PipelineError::JoinError(
            JoinError::UnsupportedJoinConstraint,
        )),
    }
}

fn parse_join_eq_expression(
    expr: &Expr,
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Vec<usize>, Vec<usize>), PipelineError> {
    let mut left_key_indexes = vec![];
    let mut right_key_indexes = vec![];
    let (left_keys, right_keys) = match expr.clone() {
        SqlExpr::Identifier(ident) => parse_identifier(
            &ConstraintIdentifier::Single(ident),
            left_join_table,
            right_join_table,
        ),
        SqlExpr::CompoundIdentifier(ident) => parse_identifier(
            &ConstraintIdentifier::Compound(ident),
            left_join_table,
            right_join_table,
        ),
        _ => {
            return Err(PipelineError::JoinError(
                JoinError::UnsupportedJoinConstraint,
            ))
        }
    }?;

    match (left_keys, right_keys) {
        (Some(left_key), None) => left_key_indexes.push(left_key),
        (None, Some(right_key)) => right_key_indexes.push(right_key),
        _ => {
            return Err(PipelineError::JoinError(
                JoinError::UnsupportedJoinConstraint,
            ))
        }
    }

    Ok((left_key_indexes, right_key_indexes))
}

fn parse_identifier(
    ident: &ConstraintIdentifier,
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Option<usize>, Option<usize>), PipelineError> {
    let is_compound = |ident: &ConstraintIdentifier| -> bool {
        match ident {
            ConstraintIdentifier::Single(_) => false,
            ConstraintIdentifier::Compound(_) => true,
        }
    };

    let left_idx = get_field_index(ident, &left_join_table.schema)?;
    let right_idx = get_field_index(ident, &right_join_table.schema)?;

    match (left_idx, right_idx) {
        (None, None) => Err(PipelineError::JoinError(JoinError::InvalidFieldSpecified(
            ident.to_string(),
        ))),
        (None, Some(idx)) => Ok((None, Some(idx))),
        (Some(idx), None) => Ok((Some(idx), None)),
        (Some(_), Some(_)) => match is_compound(ident) {
            true => Err(PipelineError::JoinError(JoinError::InvalidJoinConstraint(
                ident.to_string(),
            ))),
            false => Err(PipelineError::JoinError(JoinError::AmbiguousField(
                ident.to_string(),
            ))),
        },
    }
}

fn append_schema(
    mut output_schema: Schema,
    table: &NameOrAlias,
    current_schema: &Schema,
) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        if let Some(alias) = &table.1 {
            field.source = SourceDefinition::Alias {
                name: alias.to_string(),
            };
        }

        output_schema.fields.push(field);
    }

    output_schema
}
