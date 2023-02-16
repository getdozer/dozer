use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::{BinaryOperator, Ident, JoinConstraint, SelectItem, SetOperator, TableWithJoins};
use crate::pipeline::expression::builder::{ExpressionBuilder, NameOrAlias};
use crate::pipeline::{
    builder::SchemaSQLContext, errors::JoinError, expression::builder::extend_schema_source_def,
    product::join::JoinBranch,
};
use crate::pipeline::{
    builder::{get_input_names, IndexedTableWithJoins},
    errors::PipelineError,
};
use sqlparser::ast::Expr as SqlExpr;
use sqlparser::ast::Select;
use dozer_types::errors::internal::BoxedError;
use dozer_types::serde::Serialize;
use crate::pipeline::errors::SetError::InvalidInputSchemas;
use crate::pipeline::product::set::SetOperation;
use crate::pipeline::product::set_processor::SetProcessor;

use super::{
    join::{JoinOperator, JoinOperatorType, JoinSource, JoinTable},
    processor::FromProcessor,
};

#[derive(Debug)]
pub struct SetProcessorFactory {
    left_input_tables: IndexedTableWithJoins,
    right_input_tables: IndexedTableWithJoins,
}

impl SetProcessorFactory {
    /// Creates a new [`FromProcessorFactory`].
    pub fn new(
        left_input_tables: IndexedTableWithJoins,
        right_input_tables: IndexedTableWithJoins,
    ) -> Self {
        Self { left_input_tables, right_input_tables }
    }
}

impl ProcessorFactory<SchemaSQLContext> for SetProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        let mut input_names: Vec<NameOrAlias> = Vec::new();
        get_input_names(&self.left_input_tables).iter().for_each(|name| input_names.push(name.clone()));
        get_input_names(&self.right_input_tables).iter().for_each(|name| input_names.push(name.clone()));

        input_names
            .iter()
            .enumerate()
            .map(|(number, _)| number as PortHandle)
            .collect::<Vec<PortHandle>>()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![
            OutputPortDef::new(
                DEFAULT_PORT_HANDLE,
                OutputPortType::Stateless,
            ),
        ]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let mut output_schema = Schema::empty();

        let mut input_names: Vec<NameOrAlias> = Vec::new();
        get_input_names(&self.left_input_tables).iter().for_each(|name| input_names.push(name.clone()));
        get_input_names(&self.right_input_tables).iter().for_each(|name| input_names.push(name.clone()));

        for (port, table) in input_names.iter().enumerate() {
            if let Some((current_schema, _)) = input_schemas.get(&(port as PortHandle)) {
                if current_schema.fields.iter().map(|f| output_schema.fields.contains(f)).all(|x| !x) {
                    let current_extended_schema = extend_schema_source_def(current_schema, table);
                    output_schema = append_schema(&output_schema, &current_extended_schema);
                }
            } else {
                return Err(ExecutionError::InvalidPortHandle(port as PortHandle));
            }
        }

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        match build_set_tree(&self.left_input_tables.clone(), &self.right_input_tables.clone(), input_schemas) {
            Ok(source_names) => {
                let left_select = Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::Wildcard{ 0: Default::default() }],
                    into: None,
                    from: vec![TableWithJoins {
                        relation: self.left_input_tables.clone().relation.1,
                        joins: vec![],
                    }],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    qualify: None,
                };

                let right_select = Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::Wildcard{ 0: Default::default() }],
                    into: None,
                    from: vec![TableWithJoins {
                        relation: self.right_input_tables.clone().relation.1,
                        joins: vec![],
                    }],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    qualify: None,
                };
                let set_operation = SetOperation {
                    op: SetOperator::Union,
                    left: left_select,
                    right: right_select,
                };
                Ok(Box::new(SetProcessor::new(set_operation, source_names)))
            }
            Err(e) => {
                Err(ExecutionError::InternalStringError(e.to_string()))
            },
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
pub fn build_set_tree(
    left_tables: &IndexedTableWithJoins,
    right_tables: &IndexedTableWithJoins,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<HashMap<u16, String>, PipelineError> {
// ) {
    if (input_schemas.len() != 2) {
        return Err(PipelineError::SetError(InvalidInputSchemas))
    }

    const RIGHT_JOIN_FLAG: u32 = 0x80000000;
    let mut source_names: HashMap<u16, String> = HashMap::new();
    let mut ports: Vec<PortHandle> = Vec::new();
    input_schemas.iter().for_each(|(k, v)| ports.push(*k));
    source_names.insert(*ports.first().unwrap(), left_tables.clone().relation.0.0);
    source_names.insert(*ports.last().unwrap(), right_tables.clone().relation.0.0);

    Ok(source_names)
}

fn parse_join_constraint(
    expression: &sqlparser::ast::Expr,
    left_join_table: &Schema,
    right_join_table: &Schema,
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
                JoinError::UnsupportedJoinConstraintOperator(op.to_string()),
            )),
        },
        _ => Err(PipelineError::JoinError(
            JoinError::UnsupportedJoinConstraint(expression.to_string()),
        )),
    }
}

fn parse_join_eq_expression(
    expr: &SqlExpr,
    left_join_table: &Schema,
    right_join_table: &Schema,
) -> Result<(Vec<usize>, Vec<usize>), PipelineError> {
    let mut left_key_indexes = vec![];
    let mut right_key_indexes = vec![];
    let (left_keys, right_keys) = match expr.clone() {
        SqlExpr::Identifier(ident) => parse_identifier(&[ident], left_join_table, right_join_table),
        SqlExpr::CompoundIdentifier(ident) => {
            parse_identifier(&ident, left_join_table, right_join_table)
        }
        _ => {
            return Err(PipelineError::JoinError(
                JoinError::UnsupportedJoinConstraint(expr.clone().to_string()),
            ))
        }
    }?;

    match (left_keys, right_keys) {
        (Some(left_key), None) => left_key_indexes.push(left_key),
        (None, Some(right_key)) => right_key_indexes.push(right_key),
        _ => {
            return Err(PipelineError::JoinError(
                JoinError::UnsupportedJoinConstraint("".to_string()),
            ))
        }
    }

    Ok((left_key_indexes, right_key_indexes))
}

fn parse_identifier(
    ident: &[Ident],
    left_join_schema: &Schema,
    right_join_schema: &Schema,
) -> Result<(Option<usize>, Option<usize>), PipelineError> {
    let left_idx = get_field_index(ident, left_join_schema)?;

    let right_idx = get_field_index(ident, right_join_schema)?;

    match (left_idx, right_idx) {
        (None, None) => Err(PipelineError::JoinError(JoinError::InvalidFieldSpecified(
            ExpressionBuilder::fullname_from_ident(ident),
        ))),
        (None, Some(idx)) => Ok((None, Some(idx))),
        (Some(idx), None) => Ok((Some(idx), None)),
        (Some(_), Some(_)) => Err(PipelineError::JoinError(JoinError::InvalidJoinConstraint(
            ExpressionBuilder::fullname_from_ident(ident),
        ))),
    }
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
