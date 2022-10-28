use crate::cache::expression::{FilterExpression, Operator, QueryExpression, SortDirection};
use dozer_types::errors::cache::{CacheError, PlanError};
use dozer_types::serde_json::Value;
use dozer_types::types::{FieldDefinition, Schema};

use super::helper::{self};
use super::{IndexScan, Plan, SeqScan};

pub struct QueryPlanner<'a> {
    schema: &'a Schema,
    query: &'a QueryExpression,
    filters: Vec<(usize, Operator, Value)>,
    order_by: Vec<(usize, bool)>,
}
impl<'a> QueryPlanner<'a> {
    pub fn new(schema: &Schema, query: &QueryExpression) -> Result<Self, PlanError> {
        let mut order_by: Vec<(usize, bool)> = vec![];

        for s in query.order_by {
            let idx = Self::get_field_index(s.field_name, &schema.fields)
                .map_or(Err(PlanError::FieldNotFound), Ok)?;
            order_by.push((idx, s.direction == SortDirection::Ascending));
        }

        let mut filters = vec![];
        if let Some(filter) = query.filter {
            Self::get_ops_from_filter(schema, filter, &mut filters)?;
        }

        Ok(Self {
            // filters: IndexMap::new(),
            // range_index: HashSet::new(),
            schema,
            query,
            order_by,
            filters,
        })
    }

    fn get_field_index(field_name: String, fields: &[FieldDefinition]) -> Option<usize> {
        fields.iter().position(|f| f.name == field_name)
    }
    // fn insert_field_ops(
    //     &self,
    //     field_idx: usize,
    //     field_cmp: SimpleFilterExpression,
    // ) -> Result<(), CacheError> {
    //     match field_cmp.operator {
    //         Operator::LT | Operator::LTE => {
    //             self.range_index.insert(field_idx);
    //         }
    //         Operator::GTE | Operator::GT => {
    //             self.range_index.insert(field_idx);
    //         }
    //         Operator::EQ | Operator::Contains => {}
    //         Operator::MatchesAny | Operator::MatchesAll => {
    //             return Err(CacheError::IndexError(
    //                 dozer_types::errors::cache::IndexError::UnsupportedIndex(
    //                     field_cmp.operator.to_str().to_string(),
    //                 ),
    //             ));
    //         }
    //     }
    //     let current_ops = self
    //         .filters
    //         .get(&field_idx)
    //         .map_or(vec![], |ops| ops.to_owned());

    //     current_ops.push(field_cmp);
    //     if self.range_index.len() > 1 {
    //         return Err(CacheError::IndexError(
    //             IndexError::UnsupportedMultiRangeIndex,
    //         ));
    //     }
    //     self.filters.insert(field_idx, current_ops);
    //     Ok(())
    // }

    fn get_ops_from_filter(
        schema: &Schema,
        filter: FilterExpression,
        filters: &mut Vec<(usize, Operator, Value)>,
    ) -> Result<(), PlanError> {
        match filter {
            FilterExpression::Simple(field_name, operator, value) => {
                let idx = Self::get_field_index(field_name, &schema.fields)
                    .map_or(Err(PlanError::FieldNotFound), Ok)?;

                filters.push((idx, operator, value));
            }
            FilterExpression::And(expressions) => {
                for expr in expressions {
                    Self::get_ops_from_filter(schema, expr, filters)?;
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

    pub fn plan(&self) -> Result<Plan, CacheError> {
        let helper = helper::Helper {
            filters: self.filters,
            order_by: self.order_by,
        };

        if self.filters.is_empty() && self.order_by.is_empty() {
            Ok(Plan::SeqScan(SeqScan { direction: true }))
        } else {
            let all_indexes = helper.get_all_indexes().map_err(CacheError::PlanError)?;

            let matching_index = all_indexes
                .iter()
                .find(|i| self.index_matches(i.to_owned(), self.schema))
                .map_or(Err(PlanError::MatchingIndexNotFound), Ok)?;
            Ok(Plan::IndexScans(vec![IndexScan {
                index_def: matching_index.to_owned(),
                filters: self.filters,
            }]))
        }
    }

    fn index_matches(&self, possible_index: &Vec<(usize, bool)>, schema: &Schema) -> bool {
        false
    }
}
