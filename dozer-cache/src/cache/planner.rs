use std::collections::HashSet;

use anyhow::{bail, Context};

use crate::cache::expression::{
    ExecutionStep, FilterExpression, IndexScan, Operator, QueryExpression, SeqScan, SortDirection,
};
use dozer_types::types::{Field, FieldDefinition, IndexDefinition, IndexType, Schema};

pub struct QueryPlanner {}
impl QueryPlanner {
    fn get_field_index(&self, field_name: String, fields: &Vec<FieldDefinition>) -> Option<usize> {
        fields.iter().position(|f| f.name == field_name)
    }

    fn find_matching_index<'a>(
        &self,
        idx: Vec<usize>,
        indexes: &'a Vec<IndexDefinition>,
        direction: &Vec<bool>,
    ) -> Option<&'a IndexDefinition> {
        indexes
            .iter()
            .find(|id| id.fields == idx && id.sort_direction == *direction)
    }

    fn get_ops_from_filter(
        &self,
        schema: &Schema,
        filter: FilterExpression,
        ops: &mut Vec<(usize, Operator, Option<Field>)>,
    ) -> anyhow::Result<()> {
        match filter {
            FilterExpression::Simple(field_name, operator, field) => {
                let field_key = self
                    .get_field_index(field_name, &schema.fields)
                    .context("field_name is missing")?;

                ops.push((field_key, operator, Some(field)));
            }
            FilterExpression::And(exp1, exp2) => {
                self.get_ops_from_filter(schema, *exp1, ops)?;

                self.get_ops_from_filter(schema, *exp2, ops)?;
            }
        };
        Ok(())
    }

    fn get_index_scan(
        &self,
        ops: Vec<(usize, Operator, Option<Field>)>,
        indexes: &Vec<IndexDefinition>,
    ) -> anyhow::Result<IndexScan> {
        let mut range_index = HashSet::new();
        let mut hash_index = HashSet::new();
        let mut mapped_ops = Vec::new();
        while let Some(op) = ops.iter().next() {
            // ascending
            let direction = true;
            match op.1 {
                Operator::LT | Operator::LTE => {
                    range_index.insert(op.0);
                }
                Operator::GTE | Operator::GT => {
                    range_index.insert(op.0);
                }
                Operator::EQ => {
                    hash_index.insert(op.0);
                }
                Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
                    bail!("full text search queries are not yet supported")
                }
            }

            mapped_ops.push((op.0, (direction, op.2.clone())));
        }

        if range_index.len() > 1 {
            bail!("range queries on multiple fields are not supported ")
        } else {
            // We will need a secondary range index with matching columns, directions
            let typ = IndexType::SortedInverted;

            let (key, (direction, fields)): (Vec<usize>, (Vec<bool>, Vec<Option<Field>>)) =
                mapped_ops.iter().cloned().unzip();

            let index = indexes
                .iter()
                .find(|id| id.fields == key && id.sort_direction == *direction)
                .context(format!("compound_index is required for fields {:?}", key))?;

            Ok(IndexScan {
                index_def: index.clone(),
                fields,
            })
        }
    }

    pub fn plan(&self, schema: &Schema, query: &QueryExpression) -> anyhow::Result<ExecutionStep> {
        // construct steps based on expression
        // construct plans with query steps

        let mut ops: Vec<(usize, Operator, Option<Field>)> = vec![];

        while let Some(s) = query.order_by.iter().next() {
            let new_field_key = self
                .get_field_index(s.field_name.clone(), &schema.fields)
                .context("field_name is missing")?;

            let op = if s.direction == SortDirection::Ascending {
                Operator::GT
            } else {
                Operator::LT
            };
            ops.push((new_field_key, op, None));
        }

        match query.filter.clone() {
            Some(filter) => {
                self.get_ops_from_filter(schema, filter, &mut ops)?;
            }
            None => {}
        }
        if ops.len() < 1 {
            Ok(ExecutionStep::SeqScan(SeqScan { direction: true }))
        } else {
            Ok(ExecutionStep::IndexScan(
                self.get_index_scan(ops, &schema.secondary_indexes)?,
            ))
        }
    }
}
