use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, Utc},
    types::{Operation, Schema},
};

use crate::connector::{Error, Scn};

use super::log::LogManagerContent;

#[derive(Debug, Clone)]
pub struct Transaction {
    pub commit_scn: Scn,
    pub commit_timestamp: DateTime<Utc>,
    pub operations: Vec<(usize, Operation)>,
}

#[derive(Debug, Clone)]
pub struct Processor {
    aggregator: aggregate::Aggregator,
    parser: parse::Parser,
    mapper: map::Mapper,
}

impl Processor {
    pub fn new(
        start_scn: Scn,
        table_pair_to_index: HashMap<(String, String), usize>,
        schemas: Vec<Schema>,
    ) -> Self {
        Self {
            aggregator: aggregate::Aggregator::new(start_scn),
            parser: parse::Parser::new(table_pair_to_index),
            mapper: map::Mapper::new(schemas),
        }
    }

    pub fn process<'a>(
        &'a self,
        iterator: impl IntoIterator<Item = LogManagerContent> + 'a,
    ) -> impl Iterator<Item = Result<Transaction, Error>> + 'a {
        let csf = csf::process(iterator.into_iter());
        let transaction = self.aggregator.process(csf);
        let parse = self.parser.process(transaction);
        self.mapper.process(parse)
    }
}

mod aggregate;
mod csf;
mod map;
mod parse;
