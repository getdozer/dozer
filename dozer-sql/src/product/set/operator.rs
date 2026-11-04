use super::record_map::{CountingRecordMap, CountingRecordMapEnum};
use crate::errors::PipelineError;
use dozer_sql_expression::sqlparser::ast::{SetOperator, SetQuantifier};
use dozer_types::types::Record;

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum SetAction {
    Insert,
    Delete,
    // Update,
}

#[derive(Clone, Debug)]
pub struct SetOperation {
    pub op: SetOperator,
    pub quantifier: SetQuantifier,
}

impl SetOperation {
    pub fn _new(op: SetOperator) -> Self {
        Self {
            op,
            quantifier: SetQuantifier::None,
        }
    }

    pub fn execute(
        &self,
        action: SetAction,
        record: Record,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match (self.op, self.quantifier) {
            (SetOperator::Union, SetQuantifier::All) => Ok(vec![(action, record)]),
            (SetOperator::Union, SetQuantifier::None | SetQuantifier::Distinct) => {
                self.execute_union(action, record, record_map)
            }
            _ => Err(PipelineError::InvalidOperandType(self.op.to_string())),
        }
    }

    fn execute_union(
        &self,
        action: SetAction,
        record: Record,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        match action {
            SetAction::Insert => self.union_insert(action, record, record_map),
            SetAction::Delete => self.union_delete(action, record, record_map),
        }
    }

    fn union_insert(
        &self,
        action: SetAction,
        record: Record,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let _count = self.update_map(record.clone(), false, record_map);
        if _count == 1 {
            Ok(vec![(action, record)])
        } else {
            Ok(vec![])
        }
    }

    fn union_delete(
        &self,
        action: SetAction,
        record: Record,
        record_map: &mut CountingRecordMapEnum,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let _count = self.update_map(record.clone(), true, record_map);
        if _count == 0 {
            Ok(vec![(action, record)])
        } else {
            Ok(vec![])
        }
    }

    fn update_map(
        &self,
        record: Record,
        decr: bool,
        record_map: &mut CountingRecordMapEnum,
    ) -> u64 {
        if decr {
            record_map.remove(&record);
        } else {
            record_map.insert(&record);
        }

        record_map.estimate_count(&record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::product::set::record_map::AccurateCountingRecordMap;
    use dozer_types::types::Field;

    #[test]
    fn explicit_and_implicit_union_distinct_retract_only_the_last_copy() {
        for quantifier in [SetQuantifier::None, SetQuantifier::Distinct] {
            let union = SetOperation {
                op: SetOperator::Union,
                quantifier,
            };
            let mut counts = AccurateCountingRecordMap::new().unwrap().into();
            let row = Record::new(vec![Field::Int(1)]);
            for (action, emitted) in [
                (SetAction::Insert, true),
                (SetAction::Insert, false),
                (SetAction::Delete, false),
                (SetAction::Delete, true),
                (SetAction::Insert, true),
            ] {
                let expected = if emitted {
                    vec![(action, row.clone())]
                } else {
                    vec![]
                };
                assert_eq!(
                    union.execute(action, row.clone(), &mut counts).unwrap(),
                    expected
                );
            }
        }
    }
}
