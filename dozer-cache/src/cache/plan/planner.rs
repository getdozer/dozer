use std::collections::HashSet;

use crate::cache::expression::{
    ExecutionStep, FilterExpression, IndexScan, Operator, QueryExpression, SeqScan, SortDirection,
};
use dozer_types::errors::cache::{CacheError, IndexError, QueryError};
use dozer_types::{
    serde_json::Value,
    types::{FieldDefinition, Schema},
};
use indexmap::IndexMap;

use super::helper::{self};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimpleFilterExpression {
    pub field_name: String,
    pub operator: Operator,
    pub value: Value,
}

pub enum Plan {
    IndexScans(Vec<IndexScan>),
    /// Just split out the results in specified direction.
    SeqScan(SeqScan),
}
pub struct QueryPlanner {
    order_by: IndexMap<usize, bool>,
    filters: IndexMap<usize, Vec<SimpleFilterExpression>>,
    range_index: HashSet<usize>,
}
impl QueryPlanner {
    fn new() -> Self {
        Self {
            order_by: IndexMap::new(),
            filters: IndexMap::new(),
            range_index: HashSet::new(),
        }
    }

    fn get_field_index(&self, field_name: String, fields: &[FieldDefinition]) -> Option<usize> {
        fields.iter().position(|f| f.name == field_name)
    }
    fn insert_field_ops(
        &self,
        field_idx: usize,
        field_cmp: SimpleFilterExpression,
    ) -> Result<(), CacheError> {
        match field_cmp.operator {
            Operator::LT | Operator::LTE => {
                self.range_index.insert(field_idx);
            }
            Operator::GTE | Operator::GT => {
                self.range_index.insert(field_idx);
            }
            Operator::EQ | Operator::Contains => {}
            Operator::MatchesAny | Operator::MatchesAll => {
                return Err(CacheError::IndexError(
                    dozer_types::errors::cache::IndexError::UnsupportedIndex(
                        field_cmp.operator.to_str().to_string(),
                    ),
                ));
            }
        }
        let current_ops = self
            .filters
            .get(&field_idx)
            .map_or(vec![], |ops| ops.to_owned());

        current_ops.push(field_cmp);
        if self.range_index.len() > 1 {
            return Err(CacheError::IndexError(
                IndexError::UnsupportedMultiRangeIndex,
            ));
        }
        self.filters.insert(field_idx, current_ops);
        Ok(())
    }

    fn get_ops_from_filter(
        &self,
        schema: &Schema,
        filter: FilterExpression,
    ) -> Result<(), CacheError> {
        match filter {
            FilterExpression::Simple(field_name, operator, value) => {
                let field_idx = self
                    .get_field_index(field_name, &schema.fields)
                    .map_or(Err(CacheError::QueryError(QueryError::FieldNotFound)), Ok)?;

                self.insert_field_ops(
                    field_idx,
                    SimpleFilterExpression {
                        field_name,
                        operator,
                        value,
                    },
                )?;
            }
            FilterExpression::And(expressions) => {
                for expr in expressions {
                    self.get_ops_from_filter(schema, expr)?;
                }
            }
        };
        Ok(())
    }

    fn get_compound_err(&self, key: Vec<usize>, direction: Vec<bool>) -> String {
        key.iter()
            .enumerate()
            .map(|(idx, s)| {
                format!(
                    "field_idx: {}, direction: {}",
                    s.to_string(),
                    match direction[idx] {
                        true => "asc",
                        false => "desc",
                    }
                )
            })
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn plan(&self, schema: &Schema, query: &QueryExpression) -> Result<Plan, CacheError> {
        for s in query.order_by.clone() {
            let new_field_key = self
                .get_field_index(s.field_name.clone(), &schema.fields)
                .map_or(Err(CacheError::QueryError(QueryError::FieldNotFound)), Ok)?;

            self.order_by
                .insert(new_field_key, s.direction == SortDirection::Ascending);
        }

        if let Some(filter) = query.filter.clone() {
            self.get_ops_from_filter(schema, filter)?;
        };

        let helper = helper::Helper {
            filters: &self.filters,
            order_by: &self.order_by,
        };

        if ops.is_empty() {
            Ok(ExecutionStep::SeqScan(SeqScan { direction: true }))
        } else {
            Ok(ExecutionStep::IndexScan(
                self.get_index_scan(&ops, &schema.secondary_indexes)?,
            ))
        }
    }
}
