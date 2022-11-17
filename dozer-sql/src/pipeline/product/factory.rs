use dozer_core::dag::{
    executor_local::DEFAULT_PORT_HANDLE,
    node::{
        PortHandle, StatefulPortHandle, StatefulPortHandleOptions, StatefulProcessor,
        StatefulProcessorFactory,
    },
};
use sqlparser::ast::{TableFactor, TableWithJoins};

use crate::pipeline::{errors::PipelineError, expression::builder::normalize_ident};

use super::processor::ProductProcessor;

use crate::pipeline::product::factory::PipelineError::InvalidRelation;

pub struct ProductProcessorFactory {
    from: TableWithJoins,
}

impl ProductProcessorFactory {
    /// Creates a new [`ProductProcessorFactory`].
    pub fn new(from: TableWithJoins) -> Self {
        Self { from }
    }
}

impl StatefulProcessorFactory for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        let input_tables = get_input_tables(&self.from).unwrap();
        input_tables
            .iter()
            .enumerate()
            .map(|(number, _)| number as PortHandle)
            .collect::<Vec<PortHandle>>()
    }

    fn get_output_ports(&self) -> Vec<StatefulPortHandle> {
        vec![StatefulPortHandle::new(
            DEFAULT_PORT_HANDLE,
            StatefulPortHandleOptions::default(),
        )]
    }

    fn build(&self) -> Box<dyn StatefulProcessor> {
        Box::new(ProductProcessor::new(self.from.clone()))
    }
}

/// Returns a vector of input port handles and relative table name
///
/// # Errors
///
/// This function will return an error if it's not possible to get an input name.
pub fn get_input_tables(from: &TableWithJoins) -> Result<Vec<String>, PipelineError> {
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
fn get_input_name(relation: &TableFactor) -> Result<String, PipelineError> {
    match relation {
        TableFactor::Table { name, alias: _, .. } => {
            let input_name = name
                .0
                .iter()
                .map(normalize_ident)
                .collect::<Vec<String>>()
                .join(".");

            Ok(input_name)
        }
        _ => Err(InvalidRelation),
    }
}
