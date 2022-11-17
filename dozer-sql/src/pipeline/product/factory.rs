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

    /// Returns a vector of input port handles and relative table name
    ///
    /// # Errors
    ///
    /// This function will return an error if it's not possible to get an input name.
    pub fn get_input_tables(&self) -> Result<Vec<(PortHandle, String)>, PipelineError> {
        let mut input_tables = vec![];

        input_tables.insert(0, (0, self.get_input_name(&self.from.relation)?));
        for (index, join) in self.from.joins.iter().enumerate() {
            input_tables.insert(
                index + 1,
                (
                    (index + 1) as PortHandle,
                    self.get_input_name(&join.relation)?,
                ),
            );
        }

        Ok(input_tables)
    }

    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    fn get_input_name(&self, relation: &TableFactor) -> Result<String, PipelineError> {
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
}

impl StatefulProcessorFactory for ProductProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
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
