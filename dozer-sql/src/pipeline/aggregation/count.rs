use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};

// use crate::pipeline::aggregation::aggregator::AggregationResult;
// use crate::pipeline::errors::PipelineError;
// use crate::{deserialize, deserialize_i64};
// use dozer_core::storage::prefix_transaction::PrefixTransaction;
// use dozer_types::types::Field::Int;
// use dozer_types::types::{Field, FieldType};
//

#[derive(Debug)]
pub struct CountAggregator {
    count: i64,
}

impl CountAggregator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Aggregator for CountAggregator {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError> {
        todo!()
    }

    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        todo!()
    }

    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        self.count += 1;
        Ok(Field::Int(self.count))
    }
}

//
// impl CountAggregator {
//     const _AGGREGATOR_ID: u32 = 0x02;
//
//     pub(crate) fn _get_type() -> u32 {
//         CountAggregator::_AGGREGATOR_ID
//     }
//
//     pub(crate) fn insert(
//         cur_state: Option<&[u8]>,
//         _new: &Field,
//         _return_type: FieldType,
//         _txn: &mut PrefixTransaction,
//     ) -> Result<AggregationResult, PipelineError> {
//         let prev = deserialize_i64!(cur_state);
//         let buf = (prev + 1).to_be_bytes();
//         Ok(AggregationResult::new(
//             Self::get_value(&buf),
//             Some(Vec::from(buf)),
//         ))
//     }
//
//     pub(crate) fn update(
//         cur_state: Option<&[u8]>,
//         _old: &Field,
//         _new: &Field,
//         _return_type: FieldType,
//         _txn: &mut PrefixTransaction,
//     ) -> Result<AggregationResult, PipelineError> {
//         let prev = deserialize_i64!(cur_state);
//         let buf = (prev).to_be_bytes();
//         Ok(AggregationResult::new(
//             Self::get_value(&buf),
//             Some(Vec::from(buf)),
//         ))
//     }
//
//     pub(crate) fn delete(
//         cur_state: Option<&[u8]>,
//         _old: &Field,
//         _return_type: FieldType,
//         _txn: &mut PrefixTransaction,
//     ) -> Result<AggregationResult, PipelineError> {
//         let prev = deserialize_i64!(cur_state);
//         let buf = (prev - 1).to_be_bytes();
//         Ok(AggregationResult::new(
//             Self::get_value(&buf),
//             Some(Vec::from(buf)),
//         ))
//     }
//
//     pub(crate) fn get_value(f: &[u8]) -> Field {
//         Int(i64::from_be_bytes(deserialize!(f)))
//     }
// }
