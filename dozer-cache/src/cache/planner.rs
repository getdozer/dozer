use std::collections::HashSet;

use dozer_types::{
    errors::cache::{CacheError, IndexError, QueryError},
    log::debug,
    types::SortDirection,
};

use crate::cache::expression::{
    ExecutionStep, FilterExpression, IndexScan, Operator, QueryExpression, SeqScan,
};
use dozer_types::{
    serde_json::Value,
    types::{FieldDefinition, IndexDefinition, Schema},
};

struct ScanOp {
    id: usize,
    direction: SortDirection,
    field: Option<Value>,
}
pub struct QueryPlanner {}
impl QueryPlanner {
    fn get_field_index(&self, field_name: String, fields: &[FieldDefinition]) -> Option<usize> {
        fields.iter().position(|f| f.name == field_name)
    }
    fn get_ops_from_filter(
        &self,
        schema: &Schema,
        filter: FilterExpression,
        ops: &mut Vec<(usize, Operator, Option<Value>)>,
    ) -> Result<(), CacheError> {
        match filter {
            FilterExpression::Simple(field_name, operator, field) => {
                let field_key = self
                    .get_field_index(field_name, &schema.fields)
                    .map_or(Err(CacheError::QueryError(QueryError::FieldNotFound)), Ok)?;

                ops.push((field_key, operator, Some(field)));
            }
            FilterExpression::And(expressions) => {
                for expr in expressions {
                    self.get_ops_from_filter(schema, expr, ops)?;
                }
            }
        };
        Ok(())
    }

    fn get_index_scan(
        &self,
        ops: &Vec<(usize, Operator, Option<Value>)>,
        indexes: &[IndexDefinition],
    ) -> Result<IndexScan, CacheError> {
        let mut range_index = HashSet::new();
        let mut hash_index = HashSet::new();
        let mut mapped_ops = Vec::new();

        for op in ops {
            // ascending
            debug!("{:?}", op);
            let direction = SortDirection::Ascending;
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
                Operator::Contains => {
                    // I'm not sure what `range_index` and `hash_index` are for so not adding another `full_text_index`.
                }
                Operator::MatchesAny | Operator::MatchesAll => {
                    return Err(CacheError::IndexError(
                        dozer_types::errors::cache::IndexError::UnsupportedIndex(
                            op.1.to_str().to_string(),
                        ),
                    ));
                }
            }

            mapped_ops.push(ScanOp {
                id: op.0,
                direction,
                field: op.2.clone(),
            });
        }

        if range_index.len() > 1 {
            Err(CacheError::IndexError(
                IndexError::UnsupportedMultiRangeIndex,
            ))
        } else {
            let key: Vec<(usize, SortDirection)> =
                mapped_ops.iter().map(|o| (o.id, o.direction)).collect();
            let fields: Vec<Option<Value>> = mapped_ops.iter().map(|o| o.field.clone()).collect();

            let index = indexes
                .iter()
                .find(|id| match id {
                    IndexDefinition::SortedInverted(fields) => fields == &key,
                    IndexDefinition::FullText(field_index) => {
                        key.len() == 1 && key[0].0 == *field_index
                    }
                })
                .map_or(
                    Err(CacheError::IndexError(IndexError::MissingCompoundIndex(
                        key.iter()
                            .map(|s| s.0.to_string())
                            .collect::<Vec<String>>()
                            .join(","),
                    ))),
                    Ok,
                )?;

            Ok(IndexScan {
                index_def: index.clone(),
                fields,
            })
        }
    }

    pub fn plan(
        &self,
        schema: &Schema,
        query: &QueryExpression,
    ) -> Result<ExecutionStep, CacheError> {
        // construct steps based on expression
        // construct plans with query steps

        let mut ops: Vec<(usize, Operator, Option<Value>)> = vec![];

        for s in query.order_by.clone() {
            let new_field_key = self
                .get_field_index(s.field_name.clone(), &schema.fields)
                .map_or(Err(CacheError::QueryError(QueryError::FieldNotFound)), Ok)?;

            let op = if s.direction == SortDirection::Ascending {
                Operator::GT
            } else {
                Operator::LT
            };
            ops.push((new_field_key, op, None));
        }

        if let Some(filter) = query.filter.clone() {
            self.get_ops_from_filter(schema, filter, &mut ops)?;
        };

        if ops.is_empty() {
            Ok(ExecutionStep::SeqScan(SeqScan {
                direction: SortDirection::Ascending,
            }))
        } else {
            Ok(ExecutionStep::IndexScan(
                self.get_index_scan(&ops, &schema.secondary_indexes)?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryPlanner;
    use crate::cache::{
        expression::{self, ExecutionStep, FilterExpression, QueryExpression},
        test_utils,
    };

    use dozer_types::serde_json::Value;

    #[test]
    fn test_generate_plan_simple() {
        let schema = test_utils::schema_0();
        let planner = QueryPlanner {};
        let query = QueryExpression::new(
            Some(FilterExpression::Simple(
                "foo".to_string(),
                expression::Operator::EQ,
                Value::from("bar".to_string()),
            )),
            vec![],
            10,
            0,
        );
        if let ExecutionStep::IndexScan(index_scan) = planner.plan(&schema, &query).unwrap() {
            assert_eq!(index_scan.index_def, schema.secondary_indexes[0]);
            assert_eq!(index_scan.fields, &[Some(Value::from("bar".to_string()))]);
        } else {
            panic!("IndexScan expected")
        }
    }

    #[test]
    fn test_generate_plan_and() {
        let schema = test_utils::schema_1();
        let planner = QueryPlanner {};

        let filter = FilterExpression::And(vec![
            FilterExpression::Simple("a".to_string(), expression::Operator::EQ, Value::from(1)),
            FilterExpression::Simple(
                "b".to_string(),
                expression::Operator::EQ,
                Value::from("test".to_string()),
            ),
        ]);
        let query = QueryExpression::new(Some(filter), vec![], 10, 0);
        // Pick the 3rd index
        if let ExecutionStep::IndexScan(index_scan) = planner.plan(&schema, &query).unwrap() {
            assert_eq!(index_scan.index_def, schema.secondary_indexes[3]);
            assert_eq!(
                index_scan.fields,
                &[Some(Value::from(1)), Some(Value::from("test".to_string()))]
            );
        } else {
            panic!("IndexScan expected")
        }
    }
}
