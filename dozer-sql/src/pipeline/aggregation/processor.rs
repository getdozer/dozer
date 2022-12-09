#![allow(clippy::too_many_arguments)]
use crate::deserialize;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::ExpressionExecutor;
use crate::pipeline::{aggregation::aggregator::Aggregator, expression::execution::Expression};
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::node::{PortHandle, Processor};
use dozer_types::errors::types::TypeError;
use dozer_types::internal_err;
use dozer_types::types::{Field, Operation, Record, Schema};

use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Database, Environment, RwTransaction};
use dozer_core::storage::errors::StorageError::InvalidDatabase;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use std::{collections::HashMap, mem::size_of_val};

pub enum FieldRule {
    /// Represents a dimension field, generally used in the GROUP BY clause
    Dimension(
        /// Field to be used as a dimension in the source schema
        String,
        /// Expression for this dimension
        Box<Expression>,
        /// true of this field should be included in the list of values of the
        /// output schema, otherwise false. Generally, this value is true if the field appears
        /// in the output results in addition to being in the list of the GROUP BY fields
        bool,
        /// Name of the field, if renaming is required. If `None` the original name is retained
        Option<String>,
    ),
    /// Represents an aggregated field that will be calculated using the appropriate aggregator
    Measure(
        /// Field to be aggregated in the source schema
        String,
        /// Argument of the Aggregator
        Box<Expression>,
        /// Aggregator implementation for this measure
        Aggregator,
        /// true if this field should be included in the list of values of the
        /// output schema, otherwise false. Generally this value is true if the field appears
        /// in the output results in addition of being a condition for the HAVING condition
        bool,
        /// Name of the field, if renaming is required. If `None` the original name is retained
        Option<String>,
    ),
}

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

pub struct AggregationProcessor {
    output_field_rules: Vec<FieldRule>,
    out_dimensions: Vec<(Box<Expression>, usize)>,
    out_measures: Vec<(Box<Expression>, Box<Aggregator>, usize)>,
    pub db: Option<Database>,
    meta_db: Option<Database>,
    aggregators_db: Option<Database>,
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
    pub fn new(output_field_rules: Vec<FieldRule>, schema: &Schema) -> Self {
        let (out_measures, out_dimensions) = populate_rules(&output_field_rules, schema).unwrap();
        Self {
            output_field_rules,
            out_dimensions,
            out_measures,
            db: None,
            meta_db: None,
            aggregators_db: None,
        }
    }

    fn init_store(&mut self, txn: &mut dyn Environment) -> Result<(), PipelineError> {
        self.db = Some(txn.open_database("aggr", false)?);
        self.aggregators_db = Some(txn.open_database("aggr_data", false)?);
        self.meta_db = Some(txn.open_database("meta", false)?);
        Ok(())
    }

    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) -> Result<(), PipelineError> {
        for v in &self.out_dimensions {
            out_rec.set_value(v.1, v.0.evaluate(in_rec)?.clone());
        }
        Ok(())
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> Result<Vec<u8>, PipelineError> {
        let mut vec = Vec::with_capacity(hash.len().wrapping_add(size_of_val(&database_id)));
        vec.extend_from_slice(&database_id.to_be_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn get_counter(&self, txn: &mut dyn RwTransaction) -> Result<u32, PipelineError> {
        let meta_db = self
            .meta_db
            .as_ref()
            .ok_or(PipelineError::InternalStorageError(InvalidDatabase))?;
        let curr_ctr = match txn.get(meta_db, &COUNTER_KEY.to_be_bytes())? {
            Some(v) => u32::from_be_bytes(deserialize!(v)),
            None => 1_u32,
        };
        txn.put(
            meta_db,
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
        let val: Field = Field::from_bytes(&buf[offset..offset + val_len as usize])
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

        let sz_val = value.to_bytes();
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

        Ok((5 + sz_val.len() as usize + len, r))
    }

    fn calc_and_fill_measures(
        &self,
        txn: &mut dyn RwTransaction,
        cur_state: &Option<Vec<u8>>,
        deleted_record: Option<&Record>,
        inserted_record: Option<&Record>,
        out_rec_delete: &mut Record,
        out_rec_insert: &mut Record,
        op: AggregatorOperation,
    ) -> Result<Vec<u8>, PipelineError> {
        // array holding the list of states for all measures
        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.out_measures {
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
                    let inserted_field = measure.0.evaluate(inserted_record.unwrap())?;
                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.set_value(measure.2, curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.insert(
                            curr.state,
                            &inserted_field,
                            inserted_field.get_type(),
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.insert(
                            None,
                            &inserted_field,
                            inserted_field.get_type(),
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (prefix, r)
                    }
                }
                AggregatorOperation::Delete => {
                    let deleted_field = measure.0.evaluate(deleted_record.unwrap())?;
                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.set_value(measure.2, curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.delete(
                            curr.state,
                            &deleted_field,
                            deleted_field.get_type(),
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.delete(
                            None,
                            &deleted_field,
                            deleted_field.get_type(),
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (prefix, r)
                    }
                }
                AggregatorOperation::Update => {
                    let deleted_field = measure.0.evaluate(deleted_record.unwrap())?;
                    let updated_field = measure.0.evaluate(inserted_record.unwrap())?;

                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.set_value(measure.2, curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.update(
                            curr.state,
                            &deleted_field,
                            &updated_field,
                            deleted_field.get_type(),
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.update(
                            None,
                            &deleted_field,
                            &updated_field,
                            deleted_field.get_type(),
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (prefix, r)
                    }
                }
            };

            next_state.extend(
                &Self::encode_buffer(prefix, &next_state_slice.value, &next_state_slice.state)?.1,
            );
            out_rec_insert.set_value(measure.2, next_state_slice.value);
        }

        Ok(next_state)
    }

    fn update_segment_count(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        key: Vec<u8>,
        delta: u64,
        decr: bool,
    ) -> Result<u64, PipelineError> {
        let bytes = txn.get(db, key.as_slice())?;

        let curr_count = match bytes {
            Some(b) => u64::from_be_bytes(deserialize!(b)),
            None => 0_u64,
        };

        txn.put(
            db,
            key.as_slice(),
            (if decr {
                curr_count.wrapping_sub(delta)
            } else {
                curr_count.wrapping_add(delta)
            })
            .to_be_bytes()
            .as_slice(),
        )?;
        Ok(curr_count)
    }

    fn agg_delete(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        old: &Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());

        let record_hash = if !self.out_dimensions.is_empty() {
            get_key(old, &self.out_dimensions)?
            //old.get_key(&self.out_dimensions.iter().map(|i| i.0).collect())
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        let prev_count = self.update_segment_count(txn, db, record_count_key, 1, true)?;

        let cur_state = txn.get(db, record_key.as_slice())?;
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
            self.fill_dimensions(old, &mut out_rec_delete)?;
            Operation::Delete {
                old: out_rec_delete,
            }
        } else {
            self.fill_dimensions(old, &mut out_rec_insert)?;
            self.fill_dimensions(old, &mut out_rec_delete)?;
            Operation::Update {
                new: out_rec_insert,
                old: out_rec_delete,
            }
        };

        if prev_count > 0 {
            txn.put(db, record_key.as_slice(), new_state.as_slice())?;
        } else {
            let _ = txn.del(db, record_key.as_slice(), None)?;
        }
        Ok(res)
    }

    fn agg_insert(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        new: &Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());

        let record_hash = if !self.out_dimensions.is_empty() {
            get_key(new, &self.out_dimensions)?
            //new.get_key(&self.out_dimensions.iter().map(|i| i.0).collect())
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        self.update_segment_count(txn, db, record_count_key, 1, false)?;

        let cur_state = txn.get(db, record_key.as_slice())?;
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
            self.fill_dimensions(new, &mut out_rec_insert)?;
            Operation::Insert {
                new: out_rec_insert,
            }
        } else {
            self.fill_dimensions(new, &mut out_rec_insert)?;
            self.fill_dimensions(new, &mut out_rec_delete)?;
            Operation::Update {
                new: out_rec_insert,
                old: out_rec_delete,
            }
        };

        txn.put(db, record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    fn agg_update(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        old: &Record,
        new: &Record,
        record_hash: Vec<u8>,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());
        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let cur_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            Some(old),
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Update,
        )?;

        self.fill_dimensions(new, &mut out_rec_insert)?;
        self.fill_dimensions(old, &mut out_rec_delete)?;

        let res = Operation::Update {
            new: out_rec_insert,
            old: out_rec_delete,
        };

        txn.put(db, record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    pub fn aggregate(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        op: Operation,
    ) -> Result<Vec<Operation>, PipelineError> {
        match op {
            Operation::Insert { ref new } => Ok(vec![self.agg_insert(txn, db, new)?]),
            Operation::Delete { ref old } => Ok(vec![self.agg_delete(txn, db, old)?]),
            Operation::Update { ref old, ref new } => {
                let (old_record_hash, new_record_hash) = if self.out_dimensions.is_empty() {
                    (
                        vec![AGG_DEFAULT_DIMENSION_ID],
                        vec![AGG_DEFAULT_DIMENSION_ID],
                    )
                } else {
                    (
                        get_key(old, &self.out_dimensions)?,
                        get_key(new, &self.out_dimensions)?,
                    )
                    //let record_keys: Vec<usize> = self.out_dimensions.iter().map(|i| i.0).collect();
                    //(old.get_key(&record_keys), new.get_key(&record_keys))
                };

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(txn, db, old, new, old_record_hash)?])
                } else {
                    Ok(vec![
                        self.agg_insert(txn, db, new)?,
                        self.agg_delete(txn, db, old)?,
                    ])
                }
            }
        }
    }
}

fn get_key(
    record: &Record,
    out_dimensions: &[(Box<Expression>, usize)],
) -> Result<Vec<u8>, PipelineError> {
    let mut tot_size = 0_usize;
    let mut buffers = Vec::<Vec<u8>>::with_capacity(out_dimensions.len());

    for dimension in out_dimensions.iter() {
        let value = dimension.0.evaluate(record)?;
        let bytes = value.to_bytes();
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
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError> {
        internal_err!(self.init_store(state))
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        match &self.db {
            Some(d) => {
                let ops = internal_err!(self.aggregate(txn, d, op))?;
                for op in ops {
                    fw.send(op, DEFAULT_PORT_HANDLE)?;
                }
                Ok(())
            }
            _ => Err(ExecutionError::InvalidDatabase),
        }
    }
}

type OutputRules = (
    Vec<(Box<Expression>, Box<Aggregator>, usize)>,
    Vec<(Box<Expression>, usize)>,
);

fn populate_rules(
    output_field_rules: &[FieldRule],
    schema: &Schema,
) -> Result<OutputRules, PipelineError> {
    let mut out_measures: Vec<(Box<Expression>, Box<Aggregator>, usize)> = Vec::new();
    let mut out_dimensions: Vec<(Box<Expression>, usize)> = Vec::new();

    for rule in output_field_rules.iter().enumerate() {
        match rule.1 {
            FieldRule::Measure(idx, pre_aggr, aggr, _nullable, _name) => {
                out_measures.push((pre_aggr.clone(), Box::new(aggr.clone()), rule.0));
            }
            FieldRule::Dimension(idx, expression, _nullable, _name) => {
                out_dimensions.push((expression.clone(), rule.0));
            }
        }
    }

    Ok((out_measures, out_dimensions))
}
