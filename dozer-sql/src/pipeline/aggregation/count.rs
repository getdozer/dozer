use crate::pipeline::aggregation::aggregator::AggregationResult;
use crate::pipeline::errors::PipelineError;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::types::Field::Int;
use dozer_types::types::{Field, FieldType};

pub struct CountAggregator {}

impl CountAggregator {
    const _AGGREGATOR_ID: u32 = 0x02;

    pub(crate) fn get_return_type() -> FieldType {
        FieldType::Int
    }

    pub(crate) fn _get_type() -> u32 {
        CountAggregator::_AGGREGATOR_ID
    }

    pub(crate) fn insert(
        cur_state: Option<&[u8]>,
        _new: &Field,
        _return_type: FieldType,
        _txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        let prev = match cur_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let buf = (prev + 1).to_ne_bytes();
        Ok(AggregationResult::new(
            Self::get_value(&buf),
            Some(Vec::from(buf)),
        ))
    }

    pub(crate) fn update(
        cur_state: Option<&[u8]>,
        _old: &Field,
        _new: &Field,
        _return_type: FieldType,
        _txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        let prev = match cur_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let buf = (prev).to_ne_bytes();
        Ok(AggregationResult::new(
            Self::get_value(&buf),
            Some(Vec::from(buf)),
        ))
    }

    pub(crate) fn delete(
        cur_state: Option<&[u8]>,
        _old: &Field,
        _return_type: FieldType,
        _txn: &mut PrefixTransaction,
    ) -> Result<AggregationResult, PipelineError> {
        let prev = match cur_state {
            Some(v) => i64::from_ne_bytes(v.try_into().unwrap()),
            None => 0_i64,
        };

        let buf = (prev - 1).to_ne_bytes();
        Ok(AggregationResult::new(
            Self::get_value(&buf),
            Some(Vec::from(buf)),
        ))
    }

    pub(crate) fn get_value(f: &[u8]) -> Field {
        Int(i64::from_ne_bytes(f.try_into().unwrap()))
    }
}
