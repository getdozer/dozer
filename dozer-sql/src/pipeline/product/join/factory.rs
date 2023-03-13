use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    storage::lmdb_storage::LmdbExclusiveTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::Schema;

use crate::pipeline::builder::IndexedTableWithJoins;
use crate::pipeline::expression::builder::NameOrAlias;
use crate::pipeline::{builder::SchemaSQLContext, expression::builder::extend_schema_source_def};

use super::processor::ProductProcessor;

#[derive(Debug)]
pub struct ProductProcessorFactory {
    left: NameOrAlias,
    right: NameOrAlias,
    constraint: ProductConstraint,
}

#[derive(Debug)]
pub enum ProductType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug)]
pub struct ProductConstraint {
    join_type: ProductType,
    left: NameOrAlias,
    right: NameOrAlias,
}

impl ProductProcessorFactory {
    pub fn new(left: NameOrAlias, right: NameOrAlias, constraint: ProductConstraint) -> Self {
        Self {
            left,
            right,
            constraint,
        }
    }
}

impl ProcessorFactory<SchemaSQLContext> for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
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
        let left_schema = input_schemas
            .get(&(0 as PortHandle))
            .ok_or(ExecutionError::InternalError(
                "Invalid Product".to_string().into(),
            ))?
            .clone();

        let left_extended_schema = extend_schema_source_def(&left_schema.0, &self.left);

        let right_schema = input_schemas
            .get(&(1 as PortHandle))
            .ok_or(ExecutionError::InternalError(
                "Invalid Product".to_string().into(),
            ))?
            .clone();

        let right_extended_schema = extend_schema_source_def(&right_schema.0, &self.left);

        let output_schema = append_schema(&left_extended_schema, &right_extended_schema);

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(ProductProcessor::new()))
    }
}

pub fn get_input_names(input_tables: &IndexedTableWithJoins) -> Vec<NameOrAlias> {
    let mut input_names = vec![];
    input_names.push(input_tables.relation.0.clone());

    for join in &input_tables.joins {
        input_names.push(join.0.clone());
    }
    input_names
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
