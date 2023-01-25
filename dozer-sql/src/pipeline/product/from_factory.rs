use std::collections::HashMap;

use dozer_core::dag::{
    dag::DEFAULT_PORT_HANDLE,
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
};
use dozer_types::types::Schema;
use sqlparser::ast::JoinConstraint;

use crate::pipeline::builder::{get_input_names, IndexedTabelWithJoins};
use crate::pipeline::{builder::SchemaSQLContext, errors::JoinError};
use sqlparser::ast::Expr;

use super::{
    from_join::{JoinOperator, JoinOperatorType, JoinSource, JoinTable},
    from_processor::FromProcessor,
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
        match build_join_tree(&self.input_tables, input_schemas) {
            Ok(join_operator) => Ok(Box::new(FromProcessor::new(join_operator))),
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
pub fn build_join_tree(
    join_tables: &IndexedTabelWithJoins,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<JoinOperator, JoinError> {
    const RIGHT_JOIN_FLAG: u32 = 0x80000000;

    let port = 0 as PortHandle;
    let mut left_schema = input_schemas
        .get(&port)
        .map_or(
            Err(JoinError::InvalidJoinConstraint(
                join_tables.relation.0.clone().0,
            )),
            Ok,
        )
        .unwrap()
        .clone();

    let mut left_join_table = JoinSource::Table(JoinTable::new(port, left_schema.clone()));

    let mut join_tree_root = None;

    for (index, (relation_name, join)) in join_tables.joins.iter().enumerate() {
        let right_port = (index + 1) as PortHandle;
        let right_schema = input_schemas.get(&right_port).map_or(
            Err(JoinError::InvalidJoinConstraint(
                relation_name.0.to_string(),
            )),
            Ok,
        )?;

        let right_join_table = JoinSource::Table(JoinTable::new(right_port, right_schema.clone()));

        let join_schema = append_schema(left_schema.clone(), right_schema);

        let join_op = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint) => match constraint {
                JoinConstraint::On(expression) => JoinOperator::new(
                    JoinOperatorType::Inner,
                    vec![],
                    join_schema.clone(),
                    Box::new(left_join_table),
                    Box::new(right_join_table),
                    index as u32,
                    (index + 1) as u32 | RIGHT_JOIN_FLAG,
                ),
                _ => return Err(JoinError::UnsupportedJoinConstraint),
            },
            _ => return Err(JoinError::UnsupportedJoinType),
        };

        join_tree_root = Some(join_op.clone());
        left_schema = join_schema;
        left_join_table = JoinSource::Join(join_op);
    }

    if let Some(join_operator) = join_tree_root {
        Ok(join_operator)
    } else {
        Err(JoinError::InvalidJoinConstraint(
            join_tables.relation.0.clone().0,
        ))
    }
}

fn append_schema(mut output_schema: Schema, current_schema: &Schema) -> Schema {
    for field in current_schema.clone().fields.into_iter() {
        output_schema.fields.push(field);
    }

    output_schema
}
