use crate::pipeline::errors::PipelineError;
use crate::pipeline::{aggregation::aggregator::Aggregator, expression::execution::Expression};
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::node::{PortHandle, Processor};
use dozer_types::internal_err;
use dozer_types::types::{Field, Operation, Record, Schema};

use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Database, Environment, RwTransaction};
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

pub struct AggregationProcessor {
    output_field_rules: Vec<FieldRule>,
    out_dimensions: Vec<(usize, Box<Expression>, usize)>,
    out_measures: Vec<(usize, Box<Aggregator>, usize)>,
    db: Option<Database>,
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
        }
    }

    fn init_store(&mut self, txn: &mut dyn Environment) -> Result<(), PipelineError> {
        self.db = Some(txn.open_database("aggr", false)?);
        Ok(())
    }

    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) -> Result<(), PipelineError> {
        for v in &self.out_dimensions {
            out_rec.set_value(v.2, in_rec.get_value(v.0)?.clone());
        }
        Ok(())
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> Result<Vec<u8>, PipelineError> {
        let mut vec = Vec::with_capacity(hash.len() + size_of_val(&database_id));
        vec.extend_from_slice(&database_id.to_ne_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn calc_and_fill_measures(
        &self,
        // This represents the current state for a series of aggregators and it is encoded
        // with a leading uint16 representing the payload length, followe by the actual payload.
        // For example, let's say we want to encode teh state of SUM(a), SUM(b). Sum's state is
        // represented as a uint64. The byte buffer will be 20-bytes long and look like this:
        // uint16(SUM(a) payload len), uint64(=SUM(a) state), uint16(SUM(b) payload len), uint64(=SUM(b) state)
        curr_state: &Option<Vec<u8>>,
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
            let curr_state_slice = match curr_state {
                Some(ref e) => {
                    // Read the 2-byte len header
                    let len = u16::from_ne_bytes(e[offset..offset + 2].try_into().unwrap());
                    if len == 0 {
                        None
                    } else {
                        // Read the payload of len size
                        Some(&e[offset + 2..offset + 2 + len as usize])
                    }
                }
                None => None,
            };

            let agg_type;

            // Let the aggregator calculate the new value based on teh performed operation
            let next_state_slice = match op {
                AggregatorOperation::Insert => {
                    let field = inserted_record.unwrap().get_value(measure.0)?;
                    agg_type = field.get_type()?;
                    if let Some(e) = curr_state_slice {
                        // pass the current payload to the processor to extract the value
                        let curr_value = measure.1.get_value(e, agg_type);
                        // set the value for the old record
                        out_rec_delete.set_value(measure.2, curr_value);
                    }
                    measure.1.insert(curr_state_slice, field)?
                }
                AggregatorOperation::Delete => {
                    let field = deleted_record.unwrap().get_value(measure.0)?;
                    agg_type = field.get_type()?;
                    if let Some(e) = curr_state_slice {
                        // pass the current payload to the processor to extract the value
                        let curr_value = measure.1.get_value(e, agg_type);
                        // set the value for the old record
                        out_rec_delete.set_value(measure.2, curr_value);
                    }
                    measure.1.delete(curr_state_slice, field)?
                }
                AggregatorOperation::Update => {
                    let old = deleted_record.unwrap().get_value(measure.0)?;
                    let new = inserted_record.unwrap().get_value(measure.0)?;
                    agg_type = old.get_type()?;
                    if let Some(e) = curr_state_slice {
                        // pass the current payload to the processor to extract the value
                        let curr_value = measure.1.get_value(e, agg_type);
                        // set the value for the old record
                        out_rec_delete.set_value(measure.2, curr_value);
                    }
                    measure.1.update(curr_state_slice, old, new)?
                }
            };

            // append the new state to array
            next_state.extend((next_state_slice.len() as u16).to_ne_bytes());
            offset += next_state_slice.len() + 2;

            if !next_state_slice.is_empty() {
                let next_value = measure.1.get_value(next_state_slice.as_slice(), agg_type);
                next_state.extend(next_state_slice);
                out_rec_insert.set_value(measure.2, next_value);
            } else {
                out_rec_insert.set_value(measure.2, Field::Null);
            }
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
            Some(b) => u64::from_ne_bytes(b.try_into().unwrap()),
            None => 0_u64,
        };

        txn.put(
            db,
            key.as_slice(),
            (if decr {
                curr_count - delta
            } else {
                curr_count + delta
            })
            .to_ne_bytes()
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
            old.get_key(&self.out_dimensions.iter().map(|i| i.0).collect())?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        let prev_count = self.update_segment_count(txn, db, record_count_key, 1, true)?;

        let curr_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            &curr_state,
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
            new.get_key(&self.out_dimensions.iter().map(|i| i.0).collect())?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        self.update_segment_count(txn, db, record_count_key, 1, false)?;

        let curr_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            &curr_state,
            None,
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Insert,
        )?;

        let res = if curr_state.is_none() {
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

        let curr_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            &curr_state,
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
                    let record_keys: Vec<usize> = self.out_dimensions.iter().map(|i| i.0).collect();
                    (old.get_key(&record_keys)?, new.get_key(&record_keys)?)
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

impl Processor for AggregationProcessor {
    // fn update_schema(
    //     &mut self,
    //     output_port: PortHandle,
    //     input_schemas: &HashMap<PortHandle, Schema>,
    // ) -> Result<Schema, ExecutionError> {
    //     let input_schema = input_schemas
    //         .get(&DEFAULT_PORT_HANDLE)
    //         .ok_or(InvalidPortHandle(output_port))?;

    //     let field_rules = internal_err!(self.build(&self.select, &self.groupby, input_schema))?;

    //     self.output_field_rules = field_rules;

    //     self.populate_rules(input_schema)
    //         .map_err(|e| InternalError(Box::new(e)))?;

    //     self.build_output_schema(input_schema)
    // }

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
    Vec<(usize, Box<Aggregator>, usize)>,
    Vec<(usize, Box<Expression>, usize)>,
);

fn populate_rules(
    output_field_rules: &[FieldRule],
    schema: &Schema,
) -> Result<OutputRules, PipelineError> {
    let mut out_measures: Vec<(usize, Box<Aggregator>, usize)> = Vec::new();
    let mut out_dimensions: Vec<(usize, Box<Expression>, usize)> = Vec::new();

    for rule in output_field_rules.iter().enumerate() {
        match rule.1 {
            FieldRule::Measure(idx, aggr, _nullable, _name) => {
                out_measures.push((
                    schema.get_field_index(idx.as_str())?.0,
                    Box::new(aggr.clone()),
                    rule.0,
                ));
            }
            FieldRule::Dimension(idx, expression, _nullable, _name) => {
                out_dimensions.push((
                    schema.get_field_index(idx.as_str())?.0,
                    expression.clone(),
                    rule.0,
                ));
            }
        }
    }

    Ok((out_measures, out_dimensions))
}
