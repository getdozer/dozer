use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use lmdb::Database;
use dozer_types::types::{Record, Schema};
use crate::pipeline::errors::{PipelineError, SetError};
use sqlparser::ast::{Select, SetOperator};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_core::node::PortHandle;
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::SharedTransaction;
use dozer_types::serde_json::to_string;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetAction {
    Insert,
    Delete,
    // Update,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum SetOperatorType {
    Union,
    UnionAll,
    Except,
    Intersect,
}

impl Display for SetOperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOperatorType::Union => f.write_str("UNION"),
            SetOperatorType::UnionAll => f.write_str("UNION ALL"),
            SetOperatorType::Except => f.write_str("EXCEPT"),
            SetOperatorType::Intersect => f.write_str("INTERSECT"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SetOperation {
    pub op: SetOperator,
    pub left: Select,
    pub right: Select,
}

#[derive(Clone, Debug)]
pub struct SetTable {
    port: PortHandle,
    pub schema: Schema,
}

impl SetOperation {
    pub fn new(
        op: SetOperator,
        left: Select,
        right: Select,
    ) -> Self {
        Self {
            op,
            left,
            right,
        }
    }

    // pub fn get_sources(&self) -> Vec<PortHandle> {
    //     [
    //         self.left.get_sources().as_slice(),
    //         self.right.get_sources().as_slice(),
    //     ]
    //     .concat()
    // }

    pub fn execute(
        &self,
        action: SetAction,
        from_port: PortHandle,
        record: &Record,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let set_records = match self.op {
            SetOperator::Union => {
                return self.execute_union(action, from_port, record, readers)
            },
            _ => {
                return Err(PipelineError::InvalidOperandType((&self.op).to_string()))
            }
        };
        set_records
        // return Err(PipelineError::InvalidOperandType((&self.op).to_string()))
        // if the source port is under the left branch of the join
        // if self.left.contains(&from_port) {
        //     let mut output_records = vec![];
        //
        //     // forward the record and the current join constraints to the left source
        //     let mut left_records = self.left_source.execute(
        //         action,
        //         from_port,
        //         record,
        //         database,
        //         transaction,
        //         readers,
        //     )?;
        //
        //     // update left join index
        //     for (_join_action, left_record, left_lookup_key) in left_records.iter_mut() {
        //         let left_join_key: Vec<u8> = encode_join_key(left_record, &self.left_join_key);
        //         self.update_index(
        //             _join_action.clone(),
        //             &left_join_key,
        //             left_lookup_key,
        //             self.left_lookup_index,
        //             database,
        //             transaction,
        //         )?;
        //
        //         let join_records = match self._operator {
        //             SetOperatorType::Union => self.inner_join_left(
        //                 _join_action.clone(),
        //                 left_join_key,
        //                 database,
        //                 transaction,
        //                 readers,
        //                 left_record,
        //                 left_lookup_key,
        //             )?,
        //         };
        //
        //         output_records.extend(join_records);
        //     }
        //
        //     Ok(output_records)
        // } else if self.right_source.get_sources().contains(&from_port) {
        //     let mut output_records = vec![];
        //
        //     // forward the record and the current join constraints to the left source
        //     let mut right_records = self.right_source.execute(
        //         action,
        //         from_port,
        //         record,
        //         database,
        //         transaction,
        //         readers,
        //     )?;
        //
        //     // update right join index
        //     for (_join_action, right_record, right_lookup_key) in right_records.iter_mut() {
        //         let right_join_key: Vec<u8> = encode_join_key(right_record, &self.right_join_key);
        //         self.update_index(
        //             _join_action.clone(),
        //             &right_join_key,
        //             right_lookup_key,
        //             self.right_lookup_index,
        //             database,
        //             transaction,
        //         )?;
        //
        //         let join_records = match self._operator {
        //             SetOperatorType::Union => self.inner_join_right(
        //                 _join_action.clone(),
        //                 right_join_key,
        //                 database,
        //                 transaction,
        //                 readers,
        //                 right_record,
        //                 right_lookup_key,
        //             )?,
        //         };
        //         output_records.extend(join_records);
        //     }
        //
        //     return Ok(output_records);
        // } else {
        // }
    }

    fn execute_union(
        &self,
        action: SetAction,
        from_port: PortHandle,
        record: &Record,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
    ) -> Result<Vec<(SetAction, Record)>, PipelineError> {
        let mut output_records: Vec<(SetAction, Record)> = vec![];

        let reader = readers
            .get(&from_port)
            .ok_or(PipelineError::SetError(SetError::HistoryUnavailable(from_port)))?;

        if let Some(record) = reader
            .get(record.values[0].encode().as_slice(), record.version.unwrap())
            .map_err(|err| PipelineError::SetError(SetError::HistoryRecordNotFound(
                record.values[0].encode(),
                record.version.unwrap(),
                from_port,
                err
            )))?
        {
            output_records.push((action, record));
        }

        Ok(output_records)
    }
}
//
//     #[allow(clippy::too_many_arguments)]
//     fn inner_join_left(
//         &self,
//         action: SetAction,
//         left_join_key: Vec<u8>,
//         database: &Database,
//         transaction: &SharedTransaction,
//         readers: &HashMap<u16, Box<dyn RecordReader>>,
//         left_record: &mut Record,
//         left_lookup_key: &mut [u8],
//     ) -> Result<Vec<(SetAction, Record, Vec<u8>)>, PipelineError> {
//         let right_lookup_keys = self.read_index(
//             &left_join_key,
//             self.right_lookup_index,
//             database,
//             transaction,
//         )?;
//         let mut output_records = vec![];
//
//         for right_lookup_key in right_lookup_keys.iter() {
//             // lookup on the right branch to find matching records
//             let mut right_records =
//                 self.right_source
//                     .lookup(right_lookup_key, database, transaction, readers)?;
//
//             for (right_record, right_lookup_key) in right_records.iter_mut() {
//                 let join_record = join_records(left_record, right_record);
//                 let join_lookup_key =
//                     self.encode_join_lookup_key(left_lookup_key, right_lookup_key);
//
//                 output_records.push((action.clone(), join_record, join_lookup_key));
//             }
//         }
//         Ok(output_records)
//     }
//
//     fn get_right_matching_count(
//         &self,
//         action: &SetAction,
//         left_record: &mut Record,
//         database: &Database,
//         transaction: &SharedTransaction,
//     ) -> Result<usize, PipelineError> {
//         let left_join_key: Vec<u8> = encode_join_key(left_record, &self.left_join_key);
//         let right_lookup_keys = self.read_index(
//             &left_join_key,
//             self.right_lookup_index,
//             database,
//             transaction,
//         )?;
//         let mut records_count = right_lookup_keys.len();
//         if action == &SetAction::Insert {
//             records_count -= 1;
//         }
//         Ok(records_count)
//     }
//
//     fn get_left_matching_count(
//         &self,
//         action: &SetAction,
//         right_record: &mut Record,
//         database: &Database,
//         transaction: &SharedTransaction,
//     ) -> Result<usize, PipelineError> {
//         let right_join_key: Vec<u8> = encode_join_key(right_record, &self.right_join_key);
//         let left_lookup_keys = self.read_index(
//             &right_join_key,
//             self.left_lookup_index,
//             database,
//             transaction,
//         )?;
//         let mut records_count = left_lookup_keys.len();
//         if action == &SetAction::Insert {
//             records_count -= 1;
//         }
//         Ok(records_count)
//     }
//
//     fn lookup(
//         &self,
//         lookup_key: &[u8],
//         database: &Database,
//         transaction: &SharedTransaction,
//         readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
//     ) -> Result<Vec<(Record, Vec<u8>)>, PipelineError> {
//         let mut output_records = vec![];
//
//         let (left_loookup_key, right_lookup_key) = self.decode_join_lookup_key(lookup_key);
//
//         let mut left_records =
//             self.left_source
//                 .lookup(&left_loookup_key, database, transaction, readers)?;
//
//         let mut right_records =
//             self.right_source
//                 .lookup(&right_lookup_key, database, transaction, readers)?;
//
//         for (left_record, left_lookup_key) in left_records.iter_mut() {
//             for (right_record, right_lookup_key) in right_records.iter_mut() {
//                 let join_record = join_records(left_record, right_record);
//                 let join_lookup_key =
//                     self.encode_join_lookup_key(left_lookup_key, right_lookup_key);
//
//                 output_records.push((join_record, join_lookup_key));
//             }
//         }
//
//         Ok(output_records)
//     }
//
//     pub fn update_index(
//         &self,
//         action: SetAction,
//         key: &[u8],
//         value: &[u8],
//         prefix: u32,
//         database: &Database,
//         transaction: &SharedTransaction,
//     ) -> Result<(), PipelineError> {
//         let mut exclusive_transaction = transaction.write();
//         let mut prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);
//
//         match action {
//             SetAction::Insert => {
//                 prefix_transaction
//                     .put(*database, key, value)
//                     .map_err(|err| PipelineError::IndexPutError(key.to_vec(), value.to_vec(), err))?;
//             }
//             SetAction::Delete => {
//                 prefix_transaction
//                     .del(*database, key, Some(value))
//                     .map_err(|err| PipelineError::IndexDelError(key.to_vec(), value.to_vec(), err))?;
//             }
//         }
//
//         Ok(())
//     }
//
//     fn read_index(
//         &self,
//         join_key: &[u8],
//         prefix: u32,
//         database: &Database,
//         transaction: &SharedTransaction,
//     ) -> Result<Vec<Vec<u8>>, PipelineError> {
//         let mut join_keys = vec![];
//
//         let mut exclusive_transaction = transaction.write();
//         let right_prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);
//
//         let cursor = right_prefix_transaction
//             .open_cursor(*database)
//             .map_err(|err| PipelineError::IndexGetError(join_key.to_vec(), err))?;
//
//         if !cursor
//             .seek(join_key)
//             .map_err(|err| PipelineError::IndexGetError(join_key.to_vec(), err))?
//         {
//             return Ok(join_keys);
//         }
//
//         loop {
//             let entry = cursor
//                 .read()
//                 .map_err(|err| PipelineError::IndexGetError(join_key.to_vec(), err))?;
//
//             if entry.is_none() {
//                 break;
//             }
//
//             let (key, value) = entry.unwrap();
//             if key != join_key {
//                 break;
//             }
//
//             join_keys.push(value.to_vec());
//
//             if !cursor
//                 .next()
//                 .map_err(|err| PipelineError::IndexGetError(join_key.to_vec(), err))?
//             {
//                 break;
//             }
//         }
//
//         Ok(join_keys)
//     }
//
//     fn encode_join_lookup_key(&self, left_lookup_key: &[u8], right_lookup_key: &[u8]) -> Vec<u8> {
//         let mut composite_lookup_key = Vec::with_capacity(64);
//         composite_lookup_key.extend_from_slice(&(left_lookup_key.len() as u32).to_be_bytes());
//         composite_lookup_key.extend_from_slice(left_lookup_key);
//         composite_lookup_key.extend_from_slice(&(right_lookup_key.len() as u32).to_be_bytes());
//         composite_lookup_key.extend_from_slice(right_lookup_key);
//         composite_lookup_key
//     }
//
//     fn decode_join_lookup_key(&self, join_lookup_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
//         let mut offset = 0;
//
//         let left_length = u32::from_be_bytes([
//             join_lookup_key[offset],
//             join_lookup_key[offset + 1],
//             join_lookup_key[offset + 2],
//             join_lookup_key[offset + 3],
//         ]);
//         offset += 4;
//         let left_lookup_key = &join_lookup_key[offset..offset + left_length as usize];
//         offset += left_length as usize;
//
//         let right_length = u32::from_be_bytes([
//             join_lookup_key[offset],
//             join_lookup_key[offset + 1],
//             join_lookup_key[offset + 2],
//             join_lookup_key[offset + 3],
//         ]);
//         offset += 4;
//         let right_lookup_key = &join_lookup_key[offset..offset + right_length as usize];
//
//         (left_lookup_key.to_vec(), right_lookup_key.to_vec())
//     }
// }
//
// fn join_records(left_record: &Record, right_record: &Record) -> Record {
//     let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
//     Record::new(None, concat_values, None)
// }
//
// fn encode_join_key(record: &Record, join_keys: &[usize]) -> Vec<u8> {
//     let mut composite_lookup_key = vec![];
//     for key in join_keys.iter() {
//         let value = &record.values[*key].encode();
//         let length = value.len() as u32;
//         composite_lookup_key.extend_from_slice(&length.to_be_bytes());
//         composite_lookup_key.extend_from_slice(value.as_slice());
//     }
//     composite_lookup_key
// }
//
// impl SetOperatorType {
//     pub fn evaluate(
//         &self,
//         output_schema: &Schema,
//         left: &Expression,
//         right: &Expression,
//     ) -> Result<Vec<Expression>, PipelineError> {
//         match self {
//             SetOperatorType::Union => execute_union(output_schema, left, right),
//             _ => Err(PipelineError::UnsupportedSqlError(UnsupportedSqlError::GenericError(self.to_string())))
//         }
//     }
// }
//
// pub fn execute_union(
//     schema: &Schema,
//     left: &Expression,
//     right: &Expression,
// ) -> Result<Vec<Expression>, PipelineError> {
//
//     Ok(vec![Expression::Column{ index: 0 }])
// }
