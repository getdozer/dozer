use anyhow::{bail, Context};
use lmdb::{Database, RoTransaction, Transaction};

use super::{helper, iterator::CacheIterator};
use crate::cache::{
    expression::{FilterExpression, Operator, QueryExpression},
    plan_types::{QueryPlan, QueryPlanner},
};
use dozer_types::types::{
    Field, FieldDefinition, IndexDefinition, IndexType, Record, Schema, SchemaIdentifier,
};

pub struct LmdbQueryPlanner<'a> {
    txn: &'a RoTransaction<'a>,
    indexer_db: &'a Database,
}
impl<'a> LmdbQueryPlanner<'a> {
    pub fn new(txn: &'a RoTransaction, indexer_db: &'a Database) -> Self {
        Self { txn, indexer_db }
    }

    fn get_index_type(_comparator: &Operator) -> IndexType {
        IndexType::SortedInverted
    }

    fn _execute(&self, plan: &QueryPlan) -> anyhow::Result<Vec<Record>> {
        bail!("Not implented")
    }
}

impl<'a> QueryPlanner for LmdbQueryPlanner<'a> {
    fn plan(
        &self,
        indexes: &Vec<IndexDefinition>,
        query: &QueryExpression,
    ) -> anyhow::Result<Vec<QueryPlan>> {
        // construct steps based on expression
        // construct plans with query steps

        // let steps = vec![];

        bail!("Not implented");
    }

    fn execute(&self, schema: &Schema, query: &QueryExpression) -> anyhow::Result<Vec<Record>> {
        let plans = self.plan(&schema.secondary_indexes, &query)?;

        // Choose the optimal plan based on cost
        let optimal_plan: &QueryPlan = plans
            .iter()
            .fold(None, |current: Option<&QueryPlan>, p: &QueryPlan| {
                let (current_initial_cost, current_total_cost) = match current {
                    Some(c) => (c.initial_cost, c.total_cost),
                    None => (0, 0),
                };
                let cost = current_initial_cost + 10 * current_total_cost;
                let new_cost = p.initial_cost + 10 * p.total_cost;
                if new_cost < cost {
                    Some(p)
                } else {
                    current
                }
            })
            .context("query_plan is expected")?;

        let records = self._execute(optimal_plan)?;
        Ok(records)
    }
}
