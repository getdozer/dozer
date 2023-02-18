use std::collections::HashMap;

use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::errors::SetError;
use crate::pipeline::errors::SetError::InvalidInputSchemas;
use crate::pipeline::expression::builder::NameOrAlias;
use crate::pipeline::product::set::SetOperation;
use crate::pipeline::product::set_processor::SetProcessor;
use crate::pipeline::{
    builder::{get_input_names, IndexedTableWithJoins},
    errors::PipelineError,
};
use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{FieldDefinition, Schema, SourceDefinition};
use sqlparser::ast::Select;
use sqlparser::ast::{SelectItem, SetOperator, SetQuantifier, TableWithJoins};

#[derive(Debug)]
pub struct SetProcessorFactory {
    left_input_tables: IndexedTableWithJoins,
    right_input_tables: IndexedTableWithJoins,
    set_quantifier: SetQuantifier,
}

impl SetProcessorFactory {
    /// Creates a new [`FromProcessorFactory`].
    pub fn new(
        left_input_tables: IndexedTableWithJoins,
        right_input_tables: IndexedTableWithJoins,
        set_quantifier: SetQuantifier,
    ) -> Self {
        Self { left_input_tables, right_input_tables, set_quantifier }
    }
}

impl ProcessorFactory<SchemaSQLContext> for SetProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        let mut input_names: Vec<NameOrAlias> = Vec::new();
        get_input_names(&self.left_input_tables)
            .iter()
            .for_each(|name| input_names.push(name.clone()));
        get_input_names(&self.right_input_tables)
            .iter()
            .for_each(|name| input_names.push(name.clone()));

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
        let mut input_names: Vec<NameOrAlias> = Vec::new();
        get_input_names(&self.left_input_tables)
            .iter()
            .for_each(|name| input_names.push(name.clone()));
        get_input_names(&self.right_input_tables)
            .iter()
            .for_each(|name| input_names.push(name.clone()));

        let output_columns =
            validate_set_operation_input_schemas(input_schemas, input_names.clone())
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        let mut output_schema = Schema::empty();
        output_schema.fields = output_columns;
        output_schema.identifier = input_schemas.get(&0).unwrap().to_owned().0.identifier;
        output_schema.primary_index = input_schemas.get(&0).unwrap().to_owned().0.primary_index;

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        match build_set_tree(
            &self.left_input_tables.clone(),
            &self.right_input_tables.clone(),
            input_schemas,
        ) {
            Ok(source_names) => {
                let left_select = Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::Wildcard {
                        0: Default::default(),
                    }],
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
                    projection: vec![SelectItem::Wildcard {
                        0: Default::default(),
                    }],
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
                    quantifier: self.set_quantifier,
                };
                Ok(Box::new(SetProcessor::new(set_operation, source_names)))
            }
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
pub fn build_set_tree(
    left_tables: &IndexedTableWithJoins,
    right_tables: &IndexedTableWithJoins,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<HashMap<u16, String>, PipelineError> {
    if input_schemas.len() != 2 {
        return Err(PipelineError::SetError(InvalidInputSchemas));
    }

    let mut source_names: HashMap<u16, String> = HashMap::new();
    let mut ports: Vec<PortHandle> = Vec::new();
    input_schemas.iter().for_each(|(k, _v)| ports.push(*k));
    source_names.insert(*ports.first().unwrap(), left_tables.clone().relation.0 .0);
    source_names.insert(*ports.last().unwrap(), right_tables.clone().relation.0 .0);

    Ok(source_names)
}

fn validate_set_operation_input_schemas(
    input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    input_names: Vec<NameOrAlias>,
) -> Result<Vec<FieldDefinition>, PipelineError> {
    if input_schemas.clone().len() != input_names.len() {
        return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
    }

    let mut left_columns = input_schemas.get(&0).unwrap().to_owned().0.fields;
    let mut right_columns = input_schemas.get(&1).unwrap().to_owned().0.fields;
    left_columns.sort();
    right_columns.sort();

    let mut output_fields = Vec::new();
    for (left, right) in left_columns.iter().zip(right_columns.iter()) {
        if !is_similar_fields(left, right) {
            return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
        }
        output_fields.push(FieldDefinition::new(
            left.name.clone(),
            left.typ,
            left.nullable,
            SourceDefinition::Dynamic,
        ));
    }
    Ok(output_fields)
}

fn is_similar_fields(left: &FieldDefinition, right: &FieldDefinition) -> bool {
    left.name == right.name && left.typ == right.typ && left.nullable == right.nullable
}
