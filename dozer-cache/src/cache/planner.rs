use super::expression::{FilterExpression, QueryExpression};
use dozer_types::types::{IndexDefinition, Record, Schema};

pub trait QueryPlanner {
    fn plan(
        &self,
        indexes: &Vec<IndexDefinition>,
        query: &QueryExpression,
    ) -> anyhow::Result<Vec<QueryPlan>>;

    fn execute(&self, schema: &Schema, query: &QueryExpression) -> anyhow::Result<Vec<Record>>;
}

pub struct QueryPlan {
    pub initial_cost: usize,
    pub total_cost: usize,
    pub rows: usize,
    pub steps: Vec<QueryPlanStep>,
}

pub struct QueryPlanStep {
    pub initial_cost: usize,
    pub total_cost: usize,
    pub rows: usize,
    pub step_type: QueryPlanStepType,
}

pub enum QueryPlanStepType {
    IndexScan(String, IndexDefinition),
    SeqScan,
    Filter(String, FilterExpression),
}
