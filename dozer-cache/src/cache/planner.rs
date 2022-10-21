use std::collections::HashSet;

use anyhow::{bail, Context};
use log::debug;

use crate::cache::expression::{
    ExecutionStep, FilterExpression, IndexScan, Operator, QueryExpression, SeqScan, SortDirection,
};
use dozer_types::{
    serde_json::Value,
    types::{FieldDefinition, IndexDefinition, Schema},
};

struct ScanOp {
    id: usize,
    direction: bool,
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
    ) -> anyhow::Result<()> {
        match filter {
            FilterExpression::Simple(field_name, operator, field) => {
                let field_key = self
                    .get_field_index(field_name, &schema.fields)
                    .context("field_name is missing")?;

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
    ) -> anyhow::Result<IndexScan> {
        let mut range_index = HashSet::new();
        let mut hash_index = HashSet::new();
        let mut mapped_ops = Vec::new();

        for op in ops {
            // ascending
            debug!("{:?}", op);
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

            mapped_ops.push(ScanOp {
                id: op.0,
                direction,
                field: op.2.clone(),
            });
        }

        if range_index.len() > 1 {
            bail!("range queries on multiple fields are not supported ")
        } else {
            let key: Vec<usize> = mapped_ops.iter().map(|o| o.id).collect();
            let direction: Vec<bool> = mapped_ops.iter().map(|o| o.direction).collect();
            let fields: Vec<Option<Value>> = mapped_ops.iter().map(|o| o.field.clone()).collect();

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

        let mut ops: Vec<(usize, Operator, Option<Value>)> = vec![];

        for s in query.order_by.clone() {
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

        if let Some(filter) = query.filter.clone() {
            self.get_ops_from_filter(schema, filter, &mut ops)?;
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

#[cfg(test)]
mod tests {
    use super::QueryPlanner;
    use crate::cache::{
        expression::{self, ExecutionStep, FilterExpression, QueryExpression},
        test_utils,
    };
    use anyhow::bail;
    use dozer_types::serde_json::Value;

    #[test]
    fn test_generate_plan_simple() -> anyhow::Result<()> {
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
        if let ExecutionStep::IndexScan(index_scan) = planner.plan(&schema, &query)? {
            assert_eq!(index_scan.index_def, schema.secondary_indexes[0]);
            assert_eq!(index_scan.fields, &[Some(Value::from("bar".to_string()))]);
        } else {
            bail!("IndexScan expected")
        }

        Ok(())
    }

    #[test]
    fn test_generate_plan_and() -> anyhow::Result<()> {
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
        if let ExecutionStep::IndexScan(index_scan) = planner.plan(&schema, &query)? {
            assert_eq!(index_scan.index_def, schema.secondary_indexes[3]);
            assert_eq!(
                index_scan.fields,
                &[Some(Value::from(1)), Some(Value::from("test".to_string()))]
            );
        } else {
            bail!("IndexScan expected")
        }

        Ok(())
    }
}
