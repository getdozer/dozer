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

use crate::pipeline::aggregation::aggregator::{
    get_aggregator_from_aggregation_expression, get_aggregator_from_aggregator_type,
    get_aggregator_type_from_aggregation_expression, AggregatorType,
};
use dozer_core::epoch::Epoch;
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use lmdb::DatabaseFlags;
use std::{collections::HashMap, mem::size_of_val};

const COUNTER_KEY: u8 = 1_u8;

enum DimensionAggregationDataType {}

#[derive(Debug)]
struct AggregationState {
    count: usize,
    states: Vec<Box<dyn Aggregator>>,
}

impl AggregationState {
    pub fn new(types: &Vec<AggregatorType>) -> Self {
        Self {
            count: 0,
            states: types
                .iter()
                .map(|t| get_aggregator_from_aggregator_type(t))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct AggregationProcessor {
    dimensions: Vec<Expression>,
    measures: Vec<Expression>,
    measures_types: Vec<AggregatorType>,
    projections: Vec<Expression>,
    input_schema: Schema,
    aggregation_schema: Schema,
    states: HashMap<Vec<Field>, AggregationState>,
}

enum AggregatorOperation {
    Insert,
    Delete,
    Update,
}

impl AggregationProcessor {
    pub fn new(
        dimensions: Vec<Expression>,
        measures: Vec<Expression>,
        projections: Vec<Expression>,
        input_schema: Schema,
        aggregation_schema: Schema,
    ) -> Result<Self, PipelineError> {
        let mut aggr_types = Vec::new();
        let mut aggr_measures = Vec::new();

        for measure in measures {
            let (aggr_measure, aggr_type) =
                get_aggregator_type_from_aggregation_expression(&measure, &input_schema)?;
            aggr_measures.push(aggr_measure);
            aggr_types.push(aggr_type);
        }

        Ok(Self {
            dimensions,
            projections,
            input_schema,
            aggregation_schema,
            states: HashMap::new(),
            measures: aggr_measures,
            measures_types: aggr_types,
        })
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> Result<Vec<u8>, PipelineError> {
        let mut vec = Vec::with_capacity(hash.len().wrapping_add(size_of_val(&database_id)));
        vec.extend_from_slice(&database_id.to_be_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn calc_and_fill_measures(
        &self,
        curr_state: &mut AggregationState,
        deleted_record: Option<&Record>,
        inserted_record: Option<&Record>,
        out_rec_delete: &mut Vec<Field>,
        out_rec_insert: &mut Vec<Field>,
        op: AggregatorOperation,
    ) -> Result<Vec<u8>, PipelineError> {
        //
        for (idx, measure) in &self.measures.iter().enumerate().collect_vec() {
            let aggregator = &curr_state.states[*idx];

            match op {
                AggregatorOperation::Insert => {
                    let inserted_field =
                        measure.evaluate(inserted_record.unwrap(), &self.input_schema)?;
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
            }
        }

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

    fn agg_delete(
        &self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        old: &mut Record,
    ) -> Result<Operation, PipelineError> {
        todo!()

        // let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        // let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());
        //
        // let record_hash = if !self.dimensions.is_empty() {
        //     get_key(&self.input_schema, old, &self.dimensions)?
        // } else {
        //     vec![AGG_DEFAULT_DIMENSION_ID]
        // };
        //
        // let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;
        //
        // let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        // let prev_count = self.update_segment_count(txn, db, record_count_key, 1, true)?;
        //
        // let cur_state = txn.get(db, record_key.as_slice())?.map(|b| b.to_vec());
        // let new_state = self.calc_and_fill_measures(
        //     txn,
        //     &cur_state,
        //     Some(old),
        //     None,
        //     &mut out_rec_delete,
        //     &mut out_rec_insert,
        //     AggregatorOperation::Delete,
        // )?;
        //
        // let res = if prev_count == 1 {
        //     Operation::Delete {
        //         old: self.build_projection(old, out_rec_delete)?,
        //     }
        // } else {
        //     Operation::Update {
        //         new: self.build_projection(old, out_rec_insert)?,
        //         old: self.build_projection(old, out_rec_delete)?,
        //     }
        // };
        //
        // if prev_count == 1 {
        //     let _ = txn.del(db, record_key.as_slice(), None)?;
        // } else {
        //     txn.put(db, record_key.as_slice(), new_state.as_slice())?;
        // }
        // Ok(res)
    }

    fn agg_insert(
        &mut self,
        txn: &mut LmdbExclusiveTransaction,
        db: Database,
        new: &mut Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let key = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, new, &self.dimensions)?
        } else {
            vec![Field::Null]
        };
        let curr_state = self
            .measures_index
            .entry(key)
            .or_insert(AggregationState::new(&self.measures_types));
        curr_state.count += 1;

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
        todo!()

        // let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        // let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());
        // let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;
        //
        // let cur_state = txn.get(db, record_key.as_slice())?.map(|b| b.to_vec());
        // let new_state = self.calc_and_fill_measures(
        //     txn,
        //     &cur_state,
        //     Some(old),
        //     Some(new),
        //     &mut out_rec_delete,
        //     &mut out_rec_insert,
        //     AggregatorOperation::Update,
        // )?;
        //
        // let res = Operation::Update {
        //     new: self.build_projection(new, out_rec_insert)?,
        //     old: self.build_projection(old, out_rec_delete)?,
        // };
        //
        // txn.put(db, record_key.as_slice(), new_state.as_slice())?;
        //
        // Ok(res)
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
        &mut self,
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
) -> Result<Vec<Field>, PipelineError> {
    let mut key = Vec::<Field>::with_capacity(dimensions.len());
    for dimension in dimensions.iter() {
        key.push(dimension.evaluate(record, schema)?);
    }
    Ok(key)
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
