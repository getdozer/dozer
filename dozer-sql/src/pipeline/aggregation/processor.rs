#![allow(clippy::too_many_arguments)]
use crate::deserialize;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::ExpressionExecutor;
use crate::pipeline::{aggregation::aggregator::Aggregator, expression::execution::Expression};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::errors::ExecutionError;
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::types::TypeError;
use dozer_types::types::{Field, Operation, Record, Schema};

use crate::pipeline::aggregation::aggregator::get_aggregator_from_aggregation_expression;
use dozer_core::epoch::Epoch;
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use lmdb::DatabaseFlags;
use std::{collections::HashMap, mem::size_of_val};

const COUNTER_KEY: u8 = 1_u8;

pub(crate) struct AggregationData<'a> {
    pub value: Field,
    pub state: Option<&'a [u8]>,
    pub prefix: u32,
}

impl<'a> AggregationData<'a> {
    pub fn new(value: Field, state: Option<&'a [u8]>, prefix: u32) -> Self {
        Self {
            value,
            state,
            prefix,
        }
    }
}

#[derive(Debug)]
pub struct AggregationProcessor {
    dimensions: Vec<Expression>,
    measures: Vec<(Expression, Aggregator)>,
    projections: Vec<Expression>,
    pub db: Database,
    meta_db: Database,
    aggregators_db: Database,
    input_schema: Schema,
    aggregation_schema: Schema,
}

enum AggregatorOperation {
    Insert,
    Delete,
    Update,
}

const AGG_VALUES_DATASET_ID: u16 = 0x0000_u16;
const AGG_COUNT_DATASET_ID: u16 = 0x0001_u16;

const AGG_DEFAULT_DIMENSION_ID: u8 = 0xFF_u8;

impl AggregationProcessor {
    pub fn new(
        dimensions: Vec<Expression>,
        measures: Vec<Expression>,
        projections: Vec<Expression>,
        input_schema: Schema,
        aggregation_schema: Schema,
        txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Self, PipelineError> {
        let mut aggregators: Vec<(Expression, Aggregator)> = Vec::new();
        for measure in measures {
            aggregators.push(get_aggregator_from_aggregation_expression(
                &measure,
                &input_schema,
            )?)
        }

        Ok(Self {
            dimensions,
            measures: aggregators,
            projections,
            db: txn.create_database(Some("aggr"), Some(DatabaseFlags::empty()))?,
            meta_db: txn.create_database(Some("meta"), Some(DatabaseFlags::empty()))?,
            aggregators_db: txn.create_database(Some("aggr_data"), Some(DatabaseFlags::empty()))?,
            input_schema,
            aggregation_schema,
        })
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> Result<Vec<u8>, PipelineError> {
        let mut vec = Vec::with_capacity(hash.len().wrapping_add(size_of_val(&database_id)));
        vec.extend_from_slice(&database_id.to_be_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn get_counter(&self, txn: &mut LmdbExclusiveTransaction) -> Result<u32, PipelineError> {
        let curr_ctr = match txn.get(self.meta_db, &COUNTER_KEY.to_be_bytes())? {
            Some(v) => u32::from_be_bytes(deserialize!(v)),
            None => 1_u32,
        };
        txn.put(
            self.meta_db,
            &COUNTER_KEY.to_be_bytes(),
            &(curr_ctr + 1).to_be_bytes(),
        )?;
        Ok(curr_ctr + 1)
    }

    pub(crate) fn decode_buffer(buf: &[u8]) -> Result<(usize, AggregationData), PipelineError> {
        let prefix = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let mut offset: usize = 4;

        let val_len = u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let val: Field = Field::decode(&buf[offset..offset + val_len as usize])
            .map_err(TypeError::DeserializationError)?;
        offset += val_len as usize;
        let state_len = u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let state: Option<&[u8]> = if state_len > 0 {
            Some(&buf[offset..offset + state_len as usize])
        } else {
            None
        };
        offset += state_len as usize;

        let r = AggregationData::new(val, state, prefix);
        Ok((offset, r))
    }

    pub(crate) fn encode_buffer(
        prefix: u32,
        value: &Field,
        state: &Option<Vec<u8>>,
    ) -> Result<(usize, Vec<u8>), PipelineError> {
        let mut r = Vec::with_capacity(512);
        r.extend(prefix.to_be_bytes());

        let sz_val = value.encode();
        r.extend((sz_val.len() as u16).to_be_bytes());
        r.extend(&sz_val);

        let len = if let Some(state) = state.as_ref() {
            r.extend((state.len() as u16).to_be_bytes());
            r.extend(state);
            state.len()
        } else {
            r.extend(0_u16.to_be_bytes());
            0_usize
        };

        Ok((5 + sz_val.len() + len, r))
    }

    fn calc_and_fill_measures(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        cur_state: &Option<Vec<u8>>,
        deleted_record: Option<&Record>,
        inserted_record: Option<&Record>,
        out_rec_delete: &mut Vec<Field>,
        out_rec_insert: &mut Vec<Field>,
        op: AggregatorOperation,
    ) -> Result<Vec<u8>, PipelineError> {
        // array holding the list of states for all measures
        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.measures {
            let curr_agg_data = match cur_state {
                Some(ref e) => {
                    let (len, res) = Self::decode_buffer(&e[offset..])?;
                    offset += len;
                    Some(res)
                }
                None => None,
            };

            let (prefix, next_state_slice) = match op {
                AggregatorOperation::Insert => {
                    let inserted_field = measure
                        .0
                        .evaluate(inserted_record.unwrap(), &self.input_schema)?;
                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.push(curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.insert(
                            curr.state,
                            &inserted_field,
                            measure.0.get_type(&self.input_schema)?.return_type,
                            &mut p_tx,
                            self.aggregators_db,
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.insert(
                            None,
                            &inserted_field,
                            measure.0.get_type(&self.input_schema)?.return_type,
                            &mut p_tx,
                            self.aggregators_db,
                        )?;
                        (prefix, r)
                    }
                }
                AggregatorOperation::Delete => {
                    let deleted_field = measure
                        .0
                        .evaluate(deleted_record.unwrap(), &self.input_schema)?;
                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.push(curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.delete(
                            curr.state,
                            &deleted_field,
                            measure.0.get_type(&self.input_schema)?.return_type,
                            &mut p_tx,
                            self.aggregators_db,
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.delete(
                            None,
                            &deleted_field,
                            measure.0.get_type(&self.input_schema)?.return_type,
                            &mut p_tx,
                            self.aggregators_db,
                        )?;
                        (prefix, r)
                    }
                }
                AggregatorOperation::Update => {
                    let deleted_field = measure
                        .0
                        .evaluate(deleted_record.unwrap(), &self.input_schema)?;
                    let updated_field = measure
                        .0
                        .evaluate(inserted_record.unwrap(), &self.input_schema)?;

                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.push(curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.update(
                            curr.state,
                            &deleted_field,
                            &updated_field,
                            measure.0.get_type(&self.input_schema)?.return_type,
                            &mut p_tx,
                            self.aggregators_db,
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.update(
                            None,
                            &deleted_field,
                            &updated_field,
                            measure.0.get_type(&self.input_schema)?.return_type,
                            &mut p_tx,
                            self.aggregators_db,
                        )?;
                        (prefix, r)
                    }
                }
            };

            next_state.extend(
                &Self::encode_buffer(prefix, &next_state_slice.value, &next_state_slice.state)?.1,
            );
            out_rec_insert.push(next_state_slice.value);
        }

        Ok(next_state)
    }

    fn update_segment_count(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        key: Vec<u8>,
        delta: u64,
        decr: bool,
    ) -> Result<u64, PipelineError> {
        let bytes = txn.get(db, key.as_slice())?;

        let curr_count = match bytes {
            Some(b) => u64::from_be_bytes(deserialize!(b)),
            None => 0_u64,
        };

        let new_val = if decr {
            curr_count.wrapping_sub(delta)
        } else {
            curr_count.wrapping_add(delta)
        };

        if new_val > 0 {
            txn.put(db, key.as_slice(), new_val.to_be_bytes().as_slice())?;
        } else {
            txn.del(db, key.as_slice(), None)?;
        }
        Ok(curr_count)
    }

    fn agg_delete(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        old: &mut Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let record_hash = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, old, &self.dimensions)?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        let prev_count = self.update_segment_count(txn, db, record_count_key, 1, true)?;

        let cur_state = txn.get(db, record_key.as_slice())?.map(|b| b.to_vec());
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            Some(old),
            None,
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Delete,
        )?;

        let res = if prev_count == 1 {
            Operation::Delete {
                old: self.build_projection(old, out_rec_delete)?,
            }
        } else {
            Operation::Update {
                new: self.build_projection(old, out_rec_insert)?,
                old: self.build_projection(old, out_rec_delete)?,
            }
        };

        if prev_count == 1 {
            let _ = txn.del(db, record_key.as_slice(), None)?;
        } else {
            txn.put(db, record_key.as_slice(), new_state.as_slice())?;
        }
        Ok(res)
    }

    fn agg_insert(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        new: &mut Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let record_hash = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, new, &self.dimensions)?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        self.update_segment_count(txn, db, record_count_key, 1, false)?;

        let cur_state = txn.get(db, record_key.as_slice())?.map(|b| b.to_vec());
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            None,
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Insert,
        )?;

        let res = if cur_state.is_none() {
            Operation::Insert {
                new: self.build_projection(new, out_rec_insert)?,
            }
        } else {
            Operation::Update {
                new: self.build_projection(new, out_rec_insert)?,
                old: self.build_projection(new, out_rec_delete)?,
            }
        };

        txn.put(db, record_key.as_slice(), new_state.as_slice())?;
        Ok(res)
    }

    fn agg_update(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        old: &mut Record,
        new: &mut Record,
        record_hash: Vec<u8>,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());
        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let cur_state = txn.get(db, record_key.as_slice())?.map(|b| b.to_vec());
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            Some(old),
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Update,
        )?;

        let res = Operation::Update {
            new: self.build_projection(new, out_rec_insert)?,
            old: self.build_projection(old, out_rec_delete)?,
        };

        txn.put(db, record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    pub fn build_projection(
        &self,
        original: &mut Record,
        measures: Vec<Field>,
    ) -> Result<Record, PipelineError> {
        let original_len = original.values.len();
        original.values.extend(measures);
        let mut output = Vec::<Field>::with_capacity(self.projections.len());
        for exp in &self.projections {
            output.push(exp.evaluate(original, &self.aggregation_schema)?);
        }
        original.values.drain(original_len..);
        Ok(Record::new(None, output, None))
    }

    pub fn aggregate(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        mut op: Operation,
    ) -> Result<Vec<Operation>, PipelineError> {
        match op {
            Operation::Insert { ref mut new } => Ok(vec![self.agg_insert(txn, db, new)?]),
            Operation::Delete { ref mut old } => Ok(vec![self.agg_delete(txn, db, old)?]),
            Operation::Update {
                ref mut old,
                ref mut new,
            } => {
                let (old_record_hash, new_record_hash) = if self.dimensions.is_empty() {
                    (
                        vec![AGG_DEFAULT_DIMENSION_ID],
                        vec![AGG_DEFAULT_DIMENSION_ID],
                    )
                } else {
                    (
                        get_key(&self.input_schema, old, &self.dimensions)?,
                        get_key(&self.input_schema, new, &self.dimensions)?,
                    )
                };

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(txn, db, old, new, old_record_hash)?])
                } else {
                    Ok(vec![
                        self.agg_delete(txn, db, old)?,
                        self.agg_insert(txn, db, new)?,
                    ])
                }
            }
        }
    }
}

fn get_key(
    schema: &Schema,
    record: &Record,
    dimensions: &[Expression],
) -> Result<Vec<u8>, PipelineError> {
    let mut tot_size = 0_usize;
    let mut buffers = Vec::<Vec<u8>>::with_capacity(dimensions.len());

    for dimension in dimensions.iter() {
        let value = dimension.evaluate(record, schema)?;
        let bytes = value.encode();
        tot_size += bytes.len();
        buffers.push(bytes);
    }

    let mut res_buffer = Vec::<u8>::with_capacity(tot_size);
    for i in buffers {
        res_buffer.extend(i);
    }
    Ok(res_buffer)
}

impl Processor for AggregationProcessor {
    fn commit(&self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let ops = self
            .aggregate(&mut txn.write(), self.db, op)
            .map_err(|e| InternalError(Box::new(e)))?;
        for fop in ops {
            fw.send(fop, DEFAULT_PORT_HANDLE)?;
        }
        Ok(())
    }
}
