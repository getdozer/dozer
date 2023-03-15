use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    storage::lmdb_storage::LmdbExclusiveTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Ident, JoinConstraint as SqlJoinConstraint,
    JoinOperator as SqlJoinOperator,
};

use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::{builder::SchemaSQLContext, expression::builder::extend_schema_source_def};
use crate::pipeline::{errors::JoinError, expression::builder::NameOrAlias};

use super::{
    operator::{JoinOperator, JoinType},
    processor::ProductProcessor,
};

pub(crate) const LEFT_JOIN_PORT: PortHandle = 0;
pub(crate) const RIGHT_JOIN_PORT: PortHandle = 1;

#[derive(Debug)]
pub struct JoinProcessorFactory {
    left: Option<NameOrAlias>,
    right: Option<NameOrAlias>,
    join_operator: SqlJoinOperator,
}

impl JoinProcessorFactory {
    pub fn new(
        left: Option<NameOrAlias>,
        right: Option<NameOrAlias>,
        join_operator: SqlJoinOperator,
    ) -> Self {
        Self {
            left,
            right,
            join_operator,
        }
    }
}

impl ProcessorFactory<SchemaSQLContext> for JoinProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![LEFT_JOIN_PORT, RIGHT_JOIN_PORT]
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
        let (mut left_schema, _) = input_schemas
            .get(&(0 as PortHandle))
            .ok_or(ExecutionError::InternalError(
                "Invalid Product".to_string().into(),
            ))?
            .clone();

        if let Some(left_table_name) = &self.left {
            left_schema = extend_schema_source_def(&left_schema, left_table_name);
        }

        let (mut right_schema, _) = input_schemas
            .get(&(1 as PortHandle))
            .ok_or(ExecutionError::InternalError(
                "Invalid Product".to_string().into(),
            ))?
            .clone();

        if let Some(right_table_name) = &self.right {
            right_schema = extend_schema_source_def(&right_schema, right_table_name);
        }

        let output_schema = append_schema(&left_schema, &right_schema);

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let (join_type, join_constraint) = match &self.join_operator {
            SqlJoinOperator::Inner(constraint) => (JoinType::Inner, constraint),
            SqlJoinOperator::LeftOuter(constraint) => (JoinType::LeftOuter, constraint),
            SqlJoinOperator::RightOuter(constraint) => (JoinType::RightOuter, constraint),
            _ => {
                return Err(ExecutionError::InternalError(Box::new(
                    JoinError::UnsupportedJoinType,
                )))
            }
        };

        let expression = match join_constraint {
            SqlJoinConstraint::On(expression) => expression,
            _ => {
                return Err(ExecutionError::InternalError(Box::new(
                    JoinError::UnsupportedJoinConstraintType,
                )))
            }
        };

        let left_name = self
            .left
            .clone()
            .unwrap_or(NameOrAlias("Left".to_owned(), None));
        let left_schema =
            input_schemas
                .get(&(0 as PortHandle))
                .ok_or(ExecutionError::InternalError(Box::new(
                    JoinError::JoinBuild(left_name.0),
                )))?;

        let right_name = self
            .right
            .clone()
            .unwrap_or(NameOrAlias("Right".to_owned(), None));
        let right_schema =
            input_schemas
                .get(&(1 as PortHandle))
                .ok_or(ExecutionError::InternalError(Box::new(
                    JoinError::JoinBuild(right_name.0),
                )))?;

        let (left_join_key_indexes, right_join_key_indexes) =
            parse_join_constraint(expression, left_schema, right_schema)
                .map_err(|err| ExecutionError::InternalError(Box::new(err)))?;

        let join_operator =
            JoinOperator::new(join_type, left_join_key_indexes, right_join_key_indexes);

        Ok(Box::new(ProductProcessor::new(join_operator)))
    }
}

fn append_schema(left_schema: &Schema, right_schema: &Schema) -> Schema {
    let mut output_schema = Schema::empty();

    let left_len = left_schema.fields.len();

    for field in left_schema.fields.iter() {
        output_schema.fields.push(field.clone());
    }

    for field in right_schema.fields.iter() {
        output_schema.fields.push(field.clone());
    }

    for primary_key in left_schema.clone().primary_index.into_iter() {
        output_schema.primary_index.push(primary_key);
    }

    for primary_key in right_schema.clone().primary_index.into_iter() {
        output_schema.primary_index.push(primary_key + left_len);
    }

    output_schema
}

fn parse_join_constraint(
    expression: &sqlparser::ast::Expr,
    left_join_table: &Schema,
    right_join_table: &Schema,
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
            _ => Err(JoinError::UnsupportedJoinConstraintOperator(op.to_string())),
        },
        _ => Err(JoinError::UnsupportedJoinConstraint(expression.to_string())),
    }
}

fn parse_join_eq_expression(
    expr: &SqlExpr,
    left_join_table: &Schema,
    right_join_table: &Schema,
) -> Result<(Vec<usize>, Vec<usize>), JoinError> {
    let mut left_key_indexes = vec![];
    let mut right_key_indexes = vec![];
    let (left_keys, right_keys) = match expr.clone() {
        SqlExpr::Identifier(ident) => parse_identifier(&[ident], left_join_table, right_join_table),
        SqlExpr::CompoundIdentifier(ident) => {
            parse_identifier(&ident, left_join_table, right_join_table)
        }
        _ => {
            return Err(JoinError::UnsupportedJoinConstraint(
                expr.clone().to_string(),
            ))
        }
    }?;

    match (left_keys, right_keys) {
        (Some(left_key), None) => left_key_indexes.push(left_key),
        (None, Some(right_key)) => right_key_indexes.push(right_key),
        _ => return Err(JoinError::UnsupportedJoinConstraint("".to_string())),
    }

    Ok((left_key_indexes, right_key_indexes))
}

fn parse_identifier(
    ident: &[Ident],
    left_join_schema: &Schema,
    right_join_schema: &Schema,
) -> Result<(Option<usize>, Option<usize>), JoinError> {
    let left_idx = get_field_index(ident, left_join_schema)?;

    let right_idx = get_field_index(ident, right_join_schema)?;

    match (left_idx, right_idx) {
        (None, None) => Err(JoinError::InvalidFieldSpecified(
            ExpressionBuilder::fullname_from_ident(ident),
        )),
        (None, Some(idx)) => Ok((None, Some(idx))),
        (Some(idx), None) => Ok((Some(idx), None)),
        (Some(_), Some(_)) => Err(JoinError::InvalidJoinConstraint(
            ExpressionBuilder::fullname_from_ident(ident),
        )),
    }
}

pub fn get_field_index(ident: &[Ident], schema: &Schema) -> Result<Option<usize>, JoinError> {
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
            return Err(JoinError::NameSpaceTooLong(
                ident
                    .iter()
                    .map(|a| a.value.clone())
                    .collect::<Vec<String>>()
                    .join("."),
            ));
        }
    };
    field_index.map_or(Ok(None), |(i, _fd)| Ok(Some(i)))
}
