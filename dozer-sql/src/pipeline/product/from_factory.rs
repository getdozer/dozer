use std::{collections::HashMap, fmt::Display};

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::Schema;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Ident, JoinConstraint};

use crate::pipeline::builder::{get_input_names, IndexedTabelWithJoins, NameOrAlias};
use crate::pipeline::{builder::SchemaSQLContext, errors::JoinError};
use sqlparser::ast::Expr;

use super::{
    from_processor::FromProcessor,
    join::{JoinOperator, JoinOperatorType, JoinTable},
};

#[derive(Debug)]
pub struct FromProcessorFactory {
    input_tables: IndexedTabelWithJoins,
}

impl FromProcessorFactory {
    /// Creates a new [`FromProcessorFactory`].
    pub fn new(input_tables: IndexedTabelWithJoins) -> Self {
        Self { input_tables }
    }
}

impl ProcessorFactory<SchemaSQLContext> for FromProcessorFactory {
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
        for (port, _table) in input_names.iter().enumerate() {
            if let Some((current_schema, _)) = input_schemas.get(&(port as PortHandle)) {
                output_schema = append_schema(output_schema, current_schema);
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
            Ok(join_tables) => Ok(Box::new(FromProcessor::new(join_tables))),
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
) -> Result<HashMap<PortHandle, JoinTable>, JoinError> {
    let mut input_tables = HashMap::new();

    let port = 0;
    let input_schema = input_schemas.get(&(port as PortHandle)).map_or(
        Err(JoinError::InvalidJoinConstraint(
            join_tables.relation.0.clone().0,
        )),
        Ok,
    )?;

    let mut left_join_table = JoinTable::from(&join_tables.relation.0, input_schema);
    input_tables.insert(port as PortHandle, left_join_table.clone());

    for (index, (relation_name, join)) in join_tables.joins.iter().enumerate() {
        let input_schema = input_schemas.get(&((index + 1) as PortHandle)).map_or(
            Err(JoinError::InvalidJoinConstraint(
                join_tables.relation.0.clone().0,
            )),
            Ok,
        )?;

        let mut right_join_table = JoinTable::from(relation_name, input_schema);

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
                _ => return Err(JoinError::UnsupportedJoinConstraint),
            },
            _ => return Err(JoinError::UnsupportedJoinType),
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
) -> Result<(Vec<usize>, Vec<usize>), JoinError> {
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
            _ => Err(JoinError::UnsupportedJoinConstraint),
        },
        _ => Err(JoinError::UnsupportedJoinConstraint),
    }
}

enum ConstraintIdentifier {
    Single(Ident),
    Compound(Vec<Ident>),
}

impl Display for ConstraintIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstraintIdentifier::Single(ident) => f.write_fmt(format_args!("{}", ident)),
            ConstraintIdentifier::Compound(ident) => f.write_fmt(format_args!("{:?}", ident)),
        }
    }
}

fn parse_join_eq_expression(
    expr: &Box<Expr>,
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Vec<usize>, Vec<usize>), JoinError> {
    let mut left_key_indexes = vec![];
    let mut right_key_indexes = vec![];
    let (left_keys, right_keys) = match *expr.clone() {
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
        _ => return Err(JoinError::UnsupportedJoinConstraint),
    }?;

    match (left_keys, right_keys) {
        (Some(left_key), None) => left_key_indexes.push(left_key),
        (None, Some(right_key)) => right_key_indexes.push(right_key),
        _ => return Err(JoinError::UnsupportedJoinConstraint),
    }

    Ok((left_key_indexes, right_key_indexes))
}

fn parse_identifier(
    ident: &ConstraintIdentifier,
    left_join_table: &JoinTable,
    right_join_table: &JoinTable,
) -> Result<(Option<usize>, Option<usize>), JoinError> {
    let is_compound = |ident: &ConstraintIdentifier| -> bool {
        match ident {
            ConstraintIdentifier::Single(_) => false,
            ConstraintIdentifier::Compound(_) => true,
        }
    };

    let left_idx = get_field_idx(ident, left_join_table)?;
    let right_idx = get_field_idx(ident, right_join_table)?;

    match (left_idx, right_idx) {
        (Some(idx), None) => Ok((Some(idx), None)),
        (None, Some(idx)) => Ok((None, Some(idx))),
        (None, None) => Err(JoinError::InvalidFieldSpecified(ident.to_string())),
        (Some(_), Some(_)) => match is_compound(ident) {
            true => Err(JoinError::InvalidJoinConstraint(ident.to_string())),
            false => Err(JoinError::AmbiguousField(ident.to_string())),
        },
    }
}

fn get_field_idx(
    ident: &ConstraintIdentifier,
    join_table: &JoinTable,
) -> Result<Option<usize>, JoinError> {
    let get_field_idx = |ident: &Ident, schema: &Schema| -> Option<usize> {
        schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == ident.value)
            .map(|t| t.0)
    };

    let tables_matches = |table_ident: &Ident| -> bool {
        join_table.name.0 == table_ident.value
            || join_table
                .name
                .1
                .as_ref()
                .map_or(false, |a| *a == table_ident.value)
    };

    match ident {
        ConstraintIdentifier::Single(ident) => Ok(get_field_idx(ident, &join_table.schema)),
        ConstraintIdentifier::Compound(comp_ident) => {
            if comp_ident.len() > 2 {
                return Err(JoinError::NameSpaceTooLong(
                    comp_ident
                        .iter()
                        .map(|a| a.value.clone())
                        .collect::<Vec<String>>()
                        .join("."),
                ));
            }
            let table_name = comp_ident.first().expect("table_name is expected");
            let field_name = comp_ident.last().expect("field_name is expected");

            let table_match = tables_matches(table_name);
            let field_idx = get_field_idx(field_name, &join_table.schema);

            match (field_idx, table_match) {
                (Some(idx), true) => Ok(Some(idx)),
                (_, _) => Ok(None),
            }
        }
    }
}

fn append_schema(mut output_schema: Schema, current_schema: &Schema) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        output_schema.fields.push(field);
    }

    output_schema
}
