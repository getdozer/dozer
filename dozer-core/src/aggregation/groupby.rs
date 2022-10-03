use std::hash::Hasher;
use std::mem::size_of_val;
use ahash::AHasher;
use anyhow::anyhow;
use dozer_types::types::{Field, Operation, Record, SchemaIdentifier};
use dozer_types::types::Field::{Binary, Boolean, Bson, Decimal, Float, Int, Null, Timestamp};
use crate::aggregation::Aggregator;
use crate::state::{StateStore};


pub enum FieldRule {
    Dimension(usize),
    Measure(Box<dyn Aggregator>)
}


pub struct SizedAggregationDataset {
    dataset_id: u16,
    output_schema_id: Option<SchemaIdentifier>,
    out_fields_count: usize,
    out_dimensions: Vec<(usize, usize)>,
    out_measures: Vec<(Box<dyn Aggregator>, usize)>
}

enum AggregatorOperation {
    Insert, Delete, Update
}


impl SizedAggregationDataset {

    pub fn new(
        dataset_id: u16, store: &mut dyn StateStore,
        output_schema_id: Option<SchemaIdentifier>, output_fields: Vec<FieldRule>) -> anyhow::Result<SizedAggregationDataset> {

        let out_fields_count = output_fields.len();
        let mut out_measures: Vec<(Box<dyn Aggregator>, usize)> = Vec::new();
        let mut out_dimensions: Vec<(usize, usize)> = Vec::new();
        let mut ctr = 0;

        for rule in output_fields.into_iter() {
            match rule {
                FieldRule::Measure(aggr) => {
                    out_measures.push((aggr, ctr));
                }
                FieldRule::Dimension(idx) => {
                    out_dimensions.push((idx, ctr));
                }
            }
            ctr += 1;
        }

        store.put(&dataset_id.to_ne_bytes(), &0_u16.to_ne_bytes())?;

        Ok(SizedAggregationDataset {
            dataset_id, out_measures, out_dimensions, out_fields_count, output_schema_id
        })
    }


    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) {

        for v in &self.out_dimensions {
            out_rec.values[v.1] = in_rec.values[v.0].clone();
        }
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> anyhow::Result<Vec<u8>> {
        let mut vec = Vec::with_capacity(hash.len() + size_of_val(&database_id));
        vec.extend_from_slice(&database_id.to_ne_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn calc_and_fill_measures(
        &self, curr_state: Option<&[u8]>, deleted_record: Option<&Record>, inserted_record: Option<&Record>,
        out_rec_delete: &mut Record, out_rec_insert: &mut Record,
        op: AggregatorOperation) -> anyhow::Result<Vec<u8>> {

        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.out_measures {

            let curr_state_slice = if curr_state.is_none() { None } else {
                let len = u16::from_ne_bytes(curr_state.unwrap()[offset .. offset + 2].try_into().unwrap());
                if len == 0 { None } else {
                    Some(&curr_state.unwrap()[offset + 2..offset + 2 + len as usize])
                }
            };

            if curr_state_slice.is_some() {
                let curr_value = measure.0.get_value(curr_state_slice.unwrap());
                out_rec_delete.values[measure.1] = curr_value;
            }

            let next_state_slice = match op {
                AggregatorOperation::Insert => { measure.0.insert(curr_state_slice, &inserted_record.unwrap())? }
                AggregatorOperation::Delete => { measure.0.delete(curr_state_slice, &deleted_record.unwrap())? }
                AggregatorOperation::Update => { measure.0.update(curr_state_slice, &deleted_record.unwrap(), &inserted_record.unwrap())? }
            };

            next_state.extend((next_state_slice.len() as u16).to_ne_bytes());
            offset += next_state_slice.len() + 2;

            if next_state_slice.len() > 0 {
                let next_value = measure.0.get_value(next_state_slice.as_slice());
                next_state.extend(next_state_slice);
                out_rec_insert.values[measure.1] = next_value;
            }
            else {
                out_rec_insert.values[measure.1] = Field::Null;
            }
        }

        Ok(next_state)

    }

    fn update_segment_count(&self, store: &mut dyn StateStore, key: Vec<u8>, delta: u64, decr: bool) -> anyhow::Result<u64> {

        let bytes = store.get(key.as_slice())?;
        let curr_count = if bytes.is_some() { u64::from_ne_bytes(bytes.unwrap().try_into().unwrap()) } else {0_u64};
        store.put(key.as_slice(), (if decr {curr_count - delta} else {curr_count + delta}).to_ne_bytes().as_slice())?;
        Ok(curr_count)
    }

    fn agg_delete(&self, store: &mut dyn StateStore, old: &Record) -> anyhow::Result<Operation> {

        let mut out_rec_insert = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let mut out_rec_delete = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);

        let record_hash = old.get_key(self.out_dimensions.iter().map(|i| i.0).collect())?;
        let record_key = self.get_record_key(&record_hash, self.dataset_id)?;

        let record_count_key = self.get_record_key(&record_hash, self.dataset_id + 1)?;
        let prev_count = self.update_segment_count(store, record_count_key, 1, true)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state, Some(&old), None,
            &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Delete
        )?;

        let res =
            if prev_count == 1 {
                self.fill_dimensions(&old, &mut out_rec_delete);
                Operation::Delete {old: out_rec_delete}
            }
            else {
                self.fill_dimensions(&old, &mut out_rec_insert);
                self.fill_dimensions(&old, &mut out_rec_delete);
                Operation::Update {new: out_rec_insert, old: out_rec_delete}
            };

        if prev_count > 0 {
            store.put(record_key.as_slice(), new_state.as_slice())?;
        }
        else {
            store.del(record_key.as_slice())?
        }
        Ok(res)

    }

    fn agg_insert(&self, store: &mut dyn StateStore, new: &Record) -> anyhow::Result<Operation> {

        let mut out_rec_insert = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let mut out_rec_delete = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let record_hash = &new.get_key(self.out_dimensions.iter().map(|i| i.0).collect())?;
        let record_key = self.get_record_key(&record_hash, self.dataset_id)?;

        let record_count_key = self.get_record_key(&record_hash, self.dataset_id + 1)?;
        self.update_segment_count(store, record_count_key, 1, false)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state, None, Some(&new),
            &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Insert
        )?;

        let res =
            if curr_state.is_none() {
                self.fill_dimensions(&new, &mut out_rec_insert);
                Operation::Insert {new: out_rec_insert}
            }
            else {
                self.fill_dimensions(&new, &mut out_rec_insert);
                self.fill_dimensions(&new, &mut out_rec_delete);
                Operation::Update {new: out_rec_insert, old: out_rec_delete}
            };

        store.put(record_key.as_slice(), new_state.as_slice())?;

        Ok(res)

    }

    fn agg_update(&self, store: &mut dyn StateStore, old: &Record, new: &Record, record_hash: Vec<u8>) -> anyhow::Result<Operation> {

        let mut out_rec_insert = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let mut out_rec_delete = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let record_key = self.get_record_key(&record_hash, self.dataset_id)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state, Some(&old), Some(&new),
            &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Update
        )?;

        self.fill_dimensions(&new, &mut out_rec_insert);
        self.fill_dimensions(&old, &mut out_rec_delete);

        let res =
            Operation::Update {new: out_rec_insert, old: out_rec_delete};

        store.put(record_key.as_slice(), new_state.as_slice())?;

        Ok(res)

    }


    pub fn aggregate(&self, store: &mut dyn StateStore, op: Operation) -> anyhow::Result<Vec<Operation>> {

        match op {
            Operation::Insert {ref new} => {
                Ok(vec![self.agg_insert(store, &new)?])
            }
            Operation::Delete {ref old} => {
                Ok(vec![self.agg_delete(store, &old)?])
            }
            Operation::Update {ref old, ref new} => {

                let record_keys: Vec<usize> = self.out_dimensions.iter().map(|i| i.0).collect();
                let old_record_hash = old.get_key(record_keys.clone())?;
                let new_record_hash = new.get_key(record_keys)?;

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(store, old, new, old_record_hash)?])
                }
                else {
                    Ok(vec![self.agg_delete(store, old)?, self.agg_insert(store, &new)?])
                }
            }
            _ => { return Err(anyhow!("Invalid operation".to_string())); }
        }

    }


    fn get_accumulated(&self, _store: &mut dyn StateStore, _key: &[u8]) -> anyhow::Result<Option<Field>> {
        todo!();
    }
}


