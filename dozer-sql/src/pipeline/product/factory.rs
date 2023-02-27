use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    storage::lmdb_storage::LmdbExclusiveTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{FieldDefinition, Schema};
use sqlparser::ast::{BinaryOperator, Ident, JoinConstraint};

use crate::pipeline::{
    builder::SchemaSQLContext,
    errors::JoinError,
    expression::builder::{extend_schema_source_def, NameOrAlias},
    product::{
        join::{JoinBranch, JoinWindow},
        window_builder::window_from_relation,
    },
};
use crate::pipeline::{
    builder::{get_input_names, IndexedTableWithJoins},
    errors::PipelineError,
};
use crate::pipeline::{errors::SqlError, expression::builder::ExpressionBuilder};
use sqlparser::ast::Expr as SqlExpr;

use super::{
    join::{JoinOperator, JoinOperatorType, JoinSource, JoinTable},
    processor::FromProcessor,
};

#[derive(Debug)]
pub struct FromProcessorFactory {
    input_tables: IndexedTableWithJoins,
}

impl FromProcessorFactory {
    /// Creates a new [`FromProcessorFactory`].
    pub fn new(input_tables: IndexedTableWithJoins) -> Self {
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
        let output_schema = build_join_schema(&self.input_tables, input_schemas.clone())?;

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let build = || {
            let (join_operator, source_names) = build_join_tree(&self.input_tables, input_schemas)?;
            Ok::<Box<dyn Processor>, PipelineError>(Box::new(FromProcessor::new(
                join_operator,
                source_names,
                txn,
            )?))
        };

        build().map_err(|e| ExecutionError::InternalStringError(e.to_string()))
    }
}

pub fn build_join_schema(
    join_tables: &IndexedTableWithJoins,
    input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
) -> Result<Schema, ExecutionError> {
    let port = 0 as PortHandle;
    let relation_name = &join_tables.relation.0;
    let mut left_schema = get_source_schema(port, relation_name, join_tables, &input_schemas)?;

    for (index, (relation_name, _)) in join_tables.joins.iter().enumerate() {
        let right_port = (index + 1) as PortHandle;

        let right_schema =
            get_source_schema(right_port, relation_name, join_tables, &input_schemas)?;

        let join_schema = append_schema(&left_schema, &right_schema);

        left_schema = join_schema;
    }

    Ok(left_schema)
}

fn get_source_schema(
    port: PortHandle,
    relation_name: &NameOrAlias,
    join_tables: &IndexedTableWithJoins,
    input_schemas: &HashMap<u16, (Schema, SchemaSQLContext)>,
) -> Result<Schema, ExecutionError> {
    let left_schema = input_schemas
        .get(&port)
        .ok_or(ExecutionError::InternalError(
            "Invalid Window".to_string().into(),
        ))?
        .clone();
    let left_extended_schema = extend_schema_source_def(&left_schema.0, relation_name);

    let left_source_schema = match window_from_relation(&join_tables.relation.1, &left_schema.0)
        .map_err(|_| ExecutionError::InternalStringError("Invalid window".to_string()))?
    {
        Some(left_window) => {
            let left_window_schema = left_window
                .get_output_schema(&left_schema.0)
                .map_err(|_| ExecutionError::InternalStringError("Invalid window".to_string()))?;

            extend_schema_source_def(&left_window_schema, relation_name)
        }
        None => left_extended_schema,
    };
    Ok(left_source_schema)
}

pub fn build_join_tree(
    join_tables: &IndexedTableWithJoins,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<(JoinSource, HashMap<u16, String>), PipelineError> {
    const RIGHT_JOIN_FLAG: u32 = 0x80000000;

    let mut source_names = HashMap::new();

    let port = 0 as PortHandle;
    let relation_name = &join_tables.relation.0;
    let mut left_join_source = build_join_source(port, relation_name, join_tables, &input_schemas)?;

    let mut left_extended_schema = left_join_source.get_output_schema();

    source_names.insert(port, relation_name.0.to_owned());
    let mut join_tree_root = left_join_source.clone();

    for (index, (relation_name, join)) in join_tables.joins.iter().enumerate() {
        let right_port = (index + 1) as PortHandle;

        let right_join_source =
            build_join_source(right_port, relation_name, join_tables, &input_schemas)?;

        source_names.insert(right_port, relation_name.0.to_owned());

        let join_schema = append_schema(
            &left_extended_schema,
            &right_join_source.get_output_schema(),
        );

        let (join_type, join_constraint) = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint) => {
                (JoinOperatorType::Inner, constraint)
            }
            sqlparser::ast::JoinOperator::LeftOuter(constraint) => {
                (JoinOperatorType::LeftOuter, constraint)
            }
            sqlparser::ast::JoinOperator::RightOuter(constraint) => {
                (JoinOperatorType::RightOuter, constraint)
            }
            _ => return Err(PipelineError::JoinError(JoinError::UnsupportedJoinType)),
        };

        let expression = match join_constraint {
            JoinConstraint::On(expression) => expression,
            _ => {
                return Err(PipelineError::JoinError(
                    JoinError::UnsupportedJoinConstraintType,
                ))
            }
        };

        let (left_keys, right_keys) = parse_join_constraint(
            expression,
            &left_join_source.get_output_schema(),
            &right_join_source.get_output_schema(),
        )?;
        let join_op = JoinOperator::new(
            join_type,
            join_schema.clone(),
            JoinBranch {
                join_key: left_keys,
                source: Box::new(left_join_source),
                lookup_index: index as u32,
            },
            JoinBranch {
                join_key: right_keys,
                source: Box::new(right_join_source),
                lookup_index: (index + 1) as u32 | RIGHT_JOIN_FLAG,
            },
        );

        join_tree_root = JoinSource::Join(join_op.clone());

        left_extended_schema = join_schema;
        left_join_source = JoinSource::Join(join_op);
    }

    Ok((join_tree_root, source_names))
}

fn build_join_source(
    port: PortHandle,
    relation_name: &NameOrAlias,
    join_tables: &IndexedTableWithJoins,
    input_schemas: &HashMap<u16, Schema>,
) -> Result<JoinSource, PipelineError> {
    let left_schema = input_schemas
        .get(&port)
        .ok_or(JoinError::InvalidJoinConstraint(
            join_tables.relation.0.clone().0,
        ))?
        .clone();
    let left_extended_schema = extend_schema_source_def(&left_schema, relation_name);
    let left_join_table = JoinTable::new(port, left_extended_schema);
    let left_join_source = match window_from_relation(&join_tables.relation.1, &left_schema)? {
        Some(left_window) => {
            let left_window_schema = left_window.get_output_schema(&left_schema).map_err(|_| {
                PipelineError::SqlError(SqlError::WindowError("Invalid window".to_string()))
            })?;
            let left_extended_schema = extend_schema_source_def(&left_window_schema, relation_name);

            let window_source =
                JoinWindow::new(port, left_extended_schema, left_window, left_join_table);

            JoinSource::Window(window_source)
        }
        None => JoinSource::Table(left_join_table),
    };
    Ok(left_join_source)
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
