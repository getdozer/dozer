use dozer_types::{serde_json::Value, thiserror::Error};

use crate::cache::expression::Operator;
use itertools::Itertools;

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("Generated a plan combination that shouldnt be possible.")]
    _UnexpectedError,
    #[error("Cannot have more than one range query")]
    _RangeQueryLimit,
}

pub struct Helper {
    filters: Vec<(usize, Operator, Value)>,
    order_by: Vec<(usize, bool)>,
}

impl Helper {
    // Check if index can be used
    fn is_applicable(&self, comb: Vec<(usize, bool)>) -> Result<bool, PlanError> {
        let mut bool = true;
        let prefix_len = self.order_by.len();
        // Has to be in the same order as order_by
        if prefix_len > 0 {
            let left = comb
                .chunks(prefix_len)
                .next()
                .map_or(Err(PlanError::_UnexpectedError), |v| Ok(v))?
                .to_vec();
            bool = left == self.order_by;
        }

        // Cannot have more than one range query

        Ok(bool)
    }

    fn is_range(op: Operator) -> bool {
        match op {
            Operator::LT | Operator::LTE | Operator::GT | Operator::GTE => true,
            Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
                false
            }
        }
    }
    /// Spit out all possible secondary indexes that can answer the query.
    fn get_all_indexes(&self) -> Result<(), PlanError> {
        // get all combinations of fields+order
        let fields_idx: Vec<usize> = self
            .filters
            .to_owned()
            .into_iter()
            .map(|t| t.0)
            // chain with order
            .chain(self.order_by.to_owned().into_iter().map(|t| t.0))
            .collect();
        println!("{:?}", fields_idx);

        let range_count = self
            .filters
            .to_owned()
            .into_iter()
            .filter(|t| Self::is_range(t.1.to_owned()))
            .count();

        if range_count > 1 {
            return Err(PlanError::_RangeQueryLimit);
        }

        //combine with all both asc/desc combinations
        let combinations: Vec<Vec<(usize, bool)>> = fields_idx
            .iter()
            .map(|idx| vec![(*idx, true), (*idx, false)])
            .multi_cartesian_product()
            .collect();
        println!("{:?}", combinations);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dozer_types::serde_json::Value;

    use crate::cache::expression::Operator;

    use super::Helper;

    #[test]
    fn get_all_indexes_test() {
        let helper = Helper {
            filters: vec![(0, Operator::EQ, Value::from(1))],
            order_by: vec![(1, true)],
        };
        helper.get_all_indexes().unwrap();
    }
}
