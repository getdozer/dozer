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
        // Equality can ignore decimal scale or timestamp offset. Retract the
        // representation that was inserted, so later expressions see the same row.
        let representative = record_map
            .representative(&record)
            .unwrap_or_else(|| record.clone());
        let _count = self.update_map(record, true, record_map);
        if _count == 0 {
            Ok(vec![(action, representative)])
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
    use dozer_types::{rust_decimal::Decimal, types::Field};

    #[test]
    fn union_retracts_the_first_equal_decimal_representation() {
        let first = Record::new(vec![Field::Decimal(Decimal::new(10, 1))]);
        let second = Record::new(vec![Field::Decimal(Decimal::new(100, 2))]);
        assert_eq!(first, second);
        let encoded =
            |row: &Record| bincode::encode_to_vec(row, bincode::config::standard()).unwrap();
        assert_ne!(encoded(&first), encoded(&second));
        for quantifier in [SetQuantifier::None, SetQuantifier::Distinct] {
            let op = SetOperation {
                op: SetOperator::Union,
                quantifier,
            };
            let mut map: CountingRecordMapEnum = AccurateCountingRecordMap::new().unwrap().into();
            let inserted = op
                .execute(SetAction::Insert, first.clone(), &mut map)
                .unwrap();
            assert!(op
                .execute(SetAction::Insert, second.clone(), &mut map)
                .unwrap()
                .is_empty());
            assert!(op
                .execute(SetAction::Delete, first.clone(), &mut map)
                .unwrap()
                .is_empty());
            let deleted = op
                .execute(SetAction::Delete, second.clone(), &mut map)
                .unwrap();
            assert_eq!(deleted.len(), 1);
            assert_eq!(deleted[0].0, SetAction::Delete);
            assert_eq!(encoded(&deleted[0].1), encoded(&inserted[0].1));
        }
    }
}
