use dozer_core::errors::ExecutionError::InvalidOperation;
use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::PipelineError;
use dozer_types::types::{Field, FieldType};

pub struct CountAggregator {
    pub current_state: u64,
}

impl CountAggregator {
    pub fn new() -> Self {
        Self {
            current_state: 0_u64,
        }
    }
}

impl Aggregator for CountAggregator {
    fn update(
        &mut self,
        old: &Field,
        new: &Field,
        return_type: FieldType,
    ) -> Result<Field, PipelineError> {
        self.delete(old, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete record: {}", old))))?;
        self.insert(new, return_type).map_err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {}", new))))?;
        Ok(Field::UInt(self.current_state))
    }

    fn delete(&mut self, old: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match old.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    self.current_state += 1;
                    Ok(Field::UInt(self.current_state))
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {}", field_type, old))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete record: {}", old)))),
        }
    }

    fn insert(&mut self, new: &Field, return_type: FieldType) -> Result<Field, PipelineError> {
        match new.get_type() {
            Some(field_type) => {
                if field_type == return_type {
                    self.current_state += 1;
                    Ok(Field::UInt(self.current_state))
                }
                else {
                    Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to delete due to mismatch field type {} with record: {}", field_type, new))))
                }
            },
            None => Err(PipelineError::InternalExecutionError(InvalidOperation(format!("Failed to insert record: {}", new)))),
        }
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
