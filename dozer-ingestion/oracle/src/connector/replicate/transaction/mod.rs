use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    types::{Operation, Schema},
};

use crate::connector::{Result, Scn};

use super::log::LogMinerContent;

#[derive(Debug, Clone)]
pub struct ParsedTransaction {
    pub commit_scn: Scn,
    pub commit_timestamp: DateTime<Utc>,
    pub operations: Vec<(usize, Operation)>,
}

#[derive(Debug, Clone)]
pub struct Processor {
    aggregator: aggregate::Aggregator,
    parser: parse::Parser,
}

impl Processor {
    pub fn new(
        start_scn: Scn,
        table_pair_to_index: HashMap<(String, String), usize>,
        schemas: Vec<Schema>,
    ) -> Self {
        Self {
            aggregator: aggregate::Aggregator::new(start_scn, table_pair_to_index.keys()),
            parser: parse::Parser::new(table_pair_to_index, schemas),
        }
    }

    pub fn process<'a>(
        &'a self,
        iterator: impl IntoIterator<Item = LogMinerContent> + 'a,
    ) -> impl Iterator<Item = Result<ParsedTransaction>> + 'a {
        let csf = csf::process(iterator.into_iter());
        let transaction = self.aggregator.process(csf);
        self.parser.process(transaction)
    }
}

mod aggregate;
mod csf;
mod map;
mod parse;
