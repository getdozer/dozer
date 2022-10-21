use crate::aggregation::Aggregator;
use crate::dag::dag::PortHandle;
use crate::dag::mt_executor::DEFAULT_PORT_HANDLE;
use crate::dag::node::{Processor, ProcessorFactory};
use crate::state::{StateStore, StateStoreOptions};
use anyhow::{anyhow, Context};
use dozer_types::types::{Field, FieldDefinition, Operation, Record, Schema};
use std::collections::HashMap;
use std::mem::size_of_val;

#[derive(Clone)]
pub enum FieldRule {
    /// Represents a dimension field, generally used in the GROUP BY clause
    Dimension(
        /// Field to be used as a dimension in the source schema
        String,
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
        Box<dyn Aggregator>,
        /// true if this field should be included in the list of values of the
        /// output schema, otherwise false. Generally this value is true if the field appears
        /// in the output results in addition of being a condition for the HAVING condition
        bool,
        /// Name of the field, if renaming is required. If `None` the original name is retained
        Option<String>,
    ),
}

pub struct AggregationProcessorFactory {
    output_field_rules: Vec<FieldRule>,
}

impl AggregationProcessorFactory {
    pub fn new(output_field_rules: Vec<FieldRule>) -> Self {
        Self { output_field_rules }
    }
}

impl ProcessorFactory for AggregationProcessorFactory {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        Some(StateStoreOptions::default())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(AggregationProcessor::new(self.output_field_rules.clone()))
    }
}

pub struct AggregationProcessor {
    output_field_rules: Vec<FieldRule>,
    out_dimensions: Vec<(usize, usize)>,
    out_measures: Vec<(usize, Box<dyn Aggregator>, usize)>,
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
    pub fn new(output_field_rules: Vec<FieldRule>) -> Self {
        Self {
            output_field_rules,
            out_measures: vec![],
            out_dimensions: vec![],
        }
    }

    fn populate_rules(&mut self, schema: &Schema) -> anyhow::Result<()> {
        let mut out_measures: Vec<(usize, Box<dyn Aggregator>, usize)> = Vec::new();
        let mut out_dimensions: Vec<(usize, usize)> = Vec::new();

        for rule in self.output_field_rules.iter().enumerate() {
            match rule.1 {
                FieldRule::Measure(idx, aggr, _nullable, _name) => {
                    out_measures.push((
                        schema.get_field_index(idx.as_str())?.0,
                        aggr.clone(),
                        rule.0,
                    ));
                }
                FieldRule::Dimension(idx, _nullable, _name) => {
                    out_dimensions.push((schema.get_field_index(idx.as_str())?.0, rule.0));
                }
            }
        }

        self.out_measures = out_measures;
        self.out_dimensions = out_dimensions;

        Ok(())
    }

    fn init_store(&self, store: &mut dyn StateStore) -> anyhow::Result<()> {
        store.put(&AGG_VALUES_DATASET_ID.to_ne_bytes(), &0_u16.to_ne_bytes())
    }

    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) -> anyhow::Result<()> {
        for v in &self.out_dimensions {
            out_rec.values[v.1] = in_rec.values[v.0].clone();
        }
        Ok(())
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> anyhow::Result<Vec<u8>> {
        let mut vec = Vec::with_capacity(hash.len() + size_of_val(&database_id));
        vec.extend_from_slice(&database_id.to_ne_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn calc_and_fill_measures(
        &self,
        curr_state: Option<&[u8]>,
        deleted_record: Option<&Record>,
        inserted_record: Option<&Record>,
        out_rec_delete: &mut Record,
        out_rec_insert: &mut Record,
        op: AggregatorOperation,
    ) -> anyhow::Result<Vec<u8>> {
        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.out_measures {
            let curr_state_slice = match curr_state {
                Some(e) => {
                    let len = u16::from_ne_bytes(e[offset..offset + 2].try_into().unwrap());
                    if len == 0 {
                        None
                    } else {
                        Some(&e[offset + 2..offset + 2 + len as usize])
                    }
                }
                None => None,
            };

            if let Some(e) = curr_state_slice {
                let curr_value = measure.1.get_value(e);
                out_rec_delete.values[measure.2] = curr_value;
            }

            let next_state_slice = match op {
                AggregatorOperation::Insert => {
                    let field = inserted_record
                        .unwrap()
                        .values
                        .get(measure.0)
                        .context(anyhow!("Invalid field"))?;
                    measure.1.insert(curr_state_slice, field)?
                }
                AggregatorOperation::Delete => {
                    let field = deleted_record
                        .unwrap()
                        .values
                        .get(measure.0)
                        .context(anyhow!("Invalid field"))?;
                    measure.1.delete(curr_state_slice, field)?
                }
                AggregatorOperation::Update => {
                    let old = deleted_record
                        .unwrap()
                        .values
                        .get(measure.0)
                        .context(anyhow!("Invalid field"))?;
                    let new = inserted_record
                        .unwrap()
                        .values
                        .get(measure.0)
                        .context(anyhow!("Invalid field"))?;
                    measure.1.update(curr_state_slice, old, new)?
                }
            };

            next_state.extend((next_state_slice.len() as u16).to_ne_bytes());
            offset += next_state_slice.len() + 2;

            if !next_state_slice.is_empty() {
                let next_value = measure.1.get_value(next_state_slice.as_slice());
                next_state.extend(next_state_slice);
                out_rec_insert.values[measure.2] = next_value;
            } else {
                out_rec_insert.values[measure.2] = Field::Null;
            }
        }

        Ok(next_state)
    }

    fn update_segment_count(
        &self,
        store: &mut dyn StateStore,
        key: Vec<u8>,
        delta: u64,
        decr: bool,
    ) -> anyhow::Result<u64> {
        let bytes = store.get(key.as_slice())?;

        let curr_count = match bytes {
            Some(b) => u64::from_ne_bytes(b.try_into().unwrap()),
            None => 0_u64,
        };

        store.put(
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

    fn agg_delete(&self, store: &mut dyn StateStore, old: &Record) -> anyhow::Result<Operation> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());

        let record_hash = if !self.out_dimensions.is_empty() {
            old.get_key(self.out_dimensions.iter().map(|i| i.0).collect())?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        let prev_count = self.update_segment_count(store, record_count_key, 1, true)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state,
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
            store.put(record_key.as_slice(), new_state.as_slice())?;
        } else {
            store.del(record_key.as_slice())?
        }
        Ok(res)
    }

    fn agg_insert(&self, store: &mut dyn StateStore, new: &Record) -> anyhow::Result<Operation> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());

        let record_hash = if !self.out_dimensions.is_empty() {
            new.get_key(self.out_dimensions.iter().map(|i| i.0).collect())?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        self.update_segment_count(store, record_count_key, 1, false)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state,
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

        store.put(record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    fn agg_update(
        &self,
        store: &mut dyn StateStore,
        old: &Record,
        new: &Record,
        record_hash: Vec<u8>,
    ) -> anyhow::Result<Operation> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());
        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state,
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

        store.put(record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    pub fn aggregate(
        &self,
        store: &mut dyn StateStore,
        op: Operation,
    ) -> anyhow::Result<Vec<Operation>> {
        match op {
            Operation::Insert { ref new } => Ok(vec![self.agg_insert(store, new)?]),
            Operation::Delete { ref old } => Ok(vec![self.agg_delete(store, old)?]),
            Operation::Update { ref old, ref new } => {
                let (old_record_hash, new_record_hash) = if self.out_dimensions.is_empty() {
                    (
                        vec![AGG_DEFAULT_DIMENSION_ID],
                        vec![AGG_DEFAULT_DIMENSION_ID],
                    )
                } else {
                    let record_keys: Vec<usize> = self.out_dimensions.iter().map(|i| i.0).collect();
                    (old.get_key(record_keys.clone())?, new.get_key(record_keys)?)
                };

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(store, old, new, old_record_hash)?])
                } else {
                    Ok(vec![
                        self.agg_delete(store, old)?,
                        self.agg_insert(store, new)?,
                    ])
                }
            }
        }
    }
}

impl Processor for AggregationProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .context("Invalid port handle")?;

        self.populate_rules(input_schema)?;

        let mut output_schema = Schema::empty();

        for e in self.output_field_rules.iter().enumerate() {
            match e.1 {
                FieldRule::Dimension(idx, is_value, name) => {
                    let src_fld = input_schema.get_field_index(idx.as_str())?;
                    output_schema.fields.push(FieldDefinition::new(
                        match name {
                            Some(n) => n.clone(),
                            _ => src_fld.1.name.clone(),
                        },
                        src_fld.1.typ.clone(),
                        false,
                    ));
                    if *is_value {
                        output_schema.values.push(e.0);
                    }
                    output_schema.primary_index.push(e.0);
                }

                FieldRule::Measure(idx, aggr, is_value, name) => {
                    let src_fld = input_schema.get_field_index(idx)?;
                    output_schema.fields.push(FieldDefinition::new(
                        match name {
                            Some(n) => n.clone(),
                            _ => src_fld.1.name.clone(),
                        },
                        aggr.get_return_type(src_fld.1.typ.clone()),
                        false,
                    ));
                    if *is_value {
                        output_schema.values.push(e.0);
                    }
                }
            }
        }
        Ok(output_schema)
    }

    fn init(&mut self, state: &mut dyn StateStore) -> anyhow::Result<()> {
        self.init_store(state)
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn crate::dag::forwarder::ProcessorChannelForwarder,
        state: &mut dyn StateStore,
    ) -> anyhow::Result<()> {
        let ops = self.aggregate(state, op)?;
        state.commit()?;
        for op in ops {
            fw.send(op, DEFAULT_PORT_HANDLE)?;
        }
        Ok(())
    }
}
