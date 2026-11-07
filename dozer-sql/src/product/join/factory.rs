use std::collections::HashMap;

use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::{
    builder::{extend_schema_source_def, ExpressionBuilder, NameOrAlias},
    sqlparser::ast::{
        BinaryOperator, Expr as SqlExpr, Ident, JoinConstraint as SqlJoinConstraint,
        JoinOperator as SqlJoinOperator,
    },
};
use dozer_types::{
    errors::internal::BoxedError,
    tonic::async_trait,
    types::{FieldDefinition, Schema},
};

use crate::errors::{JoinError, PipelineError};

use super::{
    operator::{JoinOperator, JoinType},
    processor::ProductProcessor,
};

pub(crate) const LEFT_JOIN_PORT: PortHandle = 0;
pub(crate) const RIGHT_JOIN_PORT: PortHandle = 1;

// Macro wrapper preserves the 5-argument constructor call site in builder/join.rs
// while keeping JoinProcessorFactory methods concise and cohesive.
macro_rules! define_join_factory_new {
    () => {
        pub fn new(
            id: String,
            left: Option<NameOrAlias>,
            right: Option<NameOrAlias>,
            join_operator: SqlJoinOperator,
            enable_probabilistic_optimizations: bool,
        ) -> Self {
            Self {
                id,
                mode: JoinMode::Relational {
                    left,
                    right,
                    join_operator,
                },
                enable_probabilistic_optimizations,
            }
        }
    };
}

#[derive(Debug, Clone)]
enum JoinMode {
    Relational {
        left: Option<NameOrAlias>,
        right: Option<NameOrAlias>,
        join_operator: SqlJoinOperator,
    },
    InSubquery {
        left_expr: SqlExpr,
        negated: bool,
    },
}

#[derive(Debug)]
pub struct JoinProcessorFactory {
    id: String,
    mode: JoinMode,
    enable_probabilistic_optimizations: bool,
}

impl JoinProcessorFactory {
    define_join_factory_new!();

    // Construct a semi-join (IN) or anti-join (NOT IN) processor factory that filters
    // the left stream against the first projected column of a streaming subquery.
    pub fn new_in_subquery(
        id: String,
        left_expr: SqlExpr,
        negated: bool,
        enable_probabilistic_optimizations: bool,
    ) -> Self {
        Self {
            id,
            mode: JoinMode::InSubquery { left_expr, negated },
            enable_probabilistic_optimizations,
        }
    }

    fn resolve_branch_schemas(
        &self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(Schema, Schema), PipelineError> {
        let mut left_schema = input_schemas
            .get(&LEFT_JOIN_PORT)
            .ok_or_else(|| PipelineError::InternalError("Invalid Product".to_string().into()))?
            .clone();
        let mut right_schema = input_schemas
            .get(&RIGHT_JOIN_PORT)
            .ok_or_else(|| PipelineError::InternalError("Invalid Product".to_string().into()))?
            .clone();

        if let JoinMode::Relational { left, right, .. } = &self.mode {
            if let Some(left_name) = left {
                left_schema = extend_schema_source_def(&left_schema, left_name);
            }
            if let Some(right_name) = right {
                right_schema = extend_schema_source_def(&right_schema, right_name);
            }
        }

        Ok((left_schema, right_schema))
    }
}

#[async_trait]
impl ProcessorFactory for JoinProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "Join".to_string()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![LEFT_JOIN_PORT, RIGHT_JOIN_PORT]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let (left_schema, right_schema) = self.resolve_branch_schemas(input_schemas)?;
        match &self.mode {
            JoinMode::Relational { .. } => Ok(append_schema(&left_schema, &right_schema)),
            JoinMode::InSubquery { .. } => {
                if right_schema.fields.len() != 1 {
                    return Err(PipelineError::InvalidQuery(
                        "IN subquery must project exactly one column".to_string(),
                    )
                    .into());
                }
                Ok(left_schema)
            }
        }
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let (left_schema, right_schema) = self.resolve_branch_schemas(&input_schemas)?;
        let (join_type, key_pairs) = match &self.mode {
            JoinMode::Relational { join_operator, .. } => {
                build_relational_join_spec(join_operator, &left_schema, &right_schema)?
            }
            JoinMode::InSubquery { left_expr, negated } => {
                build_in_subquery_join_spec(left_expr, *negated, &left_schema, &right_schema)?
            }
        };

        let join_operator = JoinOperator::new(
            join_type,
            key_pairs,
            (&left_schema, &right_schema),
            self.enable_probabilistic_optimizations,
        )?;

        Ok(Box::new(ProductProcessor::new(
            self.id.clone(),
            join_operator,
        )))
    }
}

fn build_relational_join_spec(
    join_operator: &SqlJoinOperator,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(JoinType, (Vec<usize>, Vec<usize>)), PipelineError> {
    let (join_type, join_constraint) = match join_operator {
        SqlJoinOperator::Inner(constraint) => (JoinType::Inner, constraint),
        SqlJoinOperator::LeftOuter(constraint) => (JoinType::LeftOuter, constraint),
        SqlJoinOperator::RightOuter(constraint) => (JoinType::RightOuter, constraint),
        _ => return Err(PipelineError::JoinError(JoinError::UnsupportedJoinType)),
    };

    let SqlJoinConstraint::On(expression) = join_constraint else {
        return Err(PipelineError::JoinError(JoinError::UnsupportedJoinConstraintType));
    };

    let keys = parse_join_constraint(expression, left_schema, right_schema)?;
    Ok((join_type, keys))
}

fn build_in_subquery_join_spec(
    left_expr: &SqlExpr,
    negated: bool,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(JoinType, (Vec<usize>, Vec<usize>)), PipelineError> {
    if right_schema.fields.len() != 1 {
        return Err(PipelineError::InvalidQuery(
            "IN subquery must project exactly one column".to_string(),
        ));
    }
    let left_idx = resolve_left_subquery_key(left_expr, left_schema)?;
    let join_type = if negated {
        JoinType::LeftAnti
    } else {
        JoinType::LeftSemi
    };
    Ok((join_type, (vec![left_idx], vec![0])))
}

fn resolve_left_subquery_key(left_expr: &SqlExpr, left_schema: &Schema) -> Result<usize, JoinError> {
    let idents = match left_expr {
        SqlExpr::Identifier(ident) => vec![ident.clone()],
        SqlExpr::CompoundIdentifier(idents) => idents.clone(),
        _ => return Err(JoinError::UnsupportedJoinConstraint(left_expr.to_string())),
    };
    get_field_index(&idents, left_schema)?.ok_or_else(|| {
        JoinError::InvalidFieldSpecified(ExpressionBuilder::fullname_from_ident(&idents))
    })
}

fn append_schema(left_schema: &Schema, right_schema: &Schema) -> Schema {
    let mut output_schema = Schema::default();
    let left_len = left_schema.fields.len();

    for field in left_schema.fields.iter() {
        output_schema.fields.push(field.clone());
    }
    for field in right_schema.fields.iter() {
        output_schema.fields.push(field.clone());
    }
    for primary_key in left_schema.primary_index.iter() {
        output_schema.primary_index.push(*primary_key);
    }
    for primary_key in right_schema.primary_index.iter() {
        output_schema.primary_index.push(*primary_key + left_len);
    }

    output_schema
}

fn parse_join_constraint(
    expression: &SqlExpr,
    left_join_table: &Schema,
    right_join_table: &Schema,
) -> Result<(Vec<usize>, Vec<usize>), JoinError> {
    let SqlExpr::BinaryOp { left, op, right } = expression else {
        return Err(JoinError::UnsupportedJoinConstraint(expression.to_string()));
    };
    match op {
        BinaryOperator::And => {
            let (mut left_keys, mut right_keys) =
                parse_join_constraint(left, left_join_table, right_join_table)?;
            let (mut left_from_right, mut right_from_right) =
                parse_join_constraint(right, left_join_table, right_join_table)?;
            left_keys.append(&mut left_from_right);
            right_keys.append(&mut right_from_right);
            Ok((left_keys, right_keys))
        }
        BinaryOperator::Eq => {
            let (mut left_keys, mut right_keys) =
                parse_join_eq_expression(left, left_join_table, right_join_table)?;
            let (left_rhs, right_rhs) =
                parse_join_eq_expression(right, left_join_table, right_join_table)?;
            left_keys.extend(left_rhs);
            right_keys.extend(right_rhs);
            Ok((left_keys, right_keys))
        }
        _ => Err(JoinError::UnsupportedJoinConstraintOperator(op.to_string())),
    }
}

fn parse_join_eq_expression(
    expr: &SqlExpr,
    left_join_table: &Schema,
    right_join_table: &Schema,
) -> Result<(Vec<usize>, Vec<usize>), JoinError> {
    let (left_keys, right_keys) = match expr {
        SqlExpr::Identifier(ident) => {
            parse_identifier(std::slice::from_ref(ident), left_join_table, right_join_table)?
        }
        SqlExpr::CompoundIdentifier(ident) => {
            parse_identifier(ident, left_join_table, right_join_table)?
        }
        _ => return Err(JoinError::UnsupportedJoinConstraint(expr.to_string())),
    };

    match (left_keys, right_keys) {
        (Some(left_key), None) => Ok((vec![left_key], vec![])),
        (None, Some(right_key)) => Ok((vec![], vec![right_key])),
        _ => Err(JoinError::UnsupportedJoinConstraint(String::new())),
    }
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
    match ident.len() {
        1 => Ok(schema
            .fields
            .iter()
            .position(|f| f.name == ident[0].value)),
        2 => {
            let table_name = &ident[0];
            let field_name = &ident[1];
            Ok(schema
                .fields
                .iter()
                .position(|f| tables_matches(table_name, f) && f.name == field_name.value))
        }
        _ => Err(JoinError::NameSpaceTooLong(
            ident
                .iter()
                .map(|a| a.value.clone())
                .collect::<Vec<String>>()
                .join("."),
        )),
    }
}

fn tables_matches(table_ident: &Ident, fd: &FieldDefinition) -> bool {
    match &fd.source {
        dozer_types::types::SourceDefinition::Table { name, .. } => name == &table_ident.value,
        dozer_types::types::SourceDefinition::Alias { name } => name == &table_ident.value,
        dozer_types::types::SourceDefinition::Dynamic => false,
    }
}
