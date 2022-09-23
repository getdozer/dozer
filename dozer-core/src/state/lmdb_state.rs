use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hasher;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use ahash::AHasher;
use lmdb::{Database, DatabaseFlags, Environment, Error, RwTransaction, Transaction, WriteFlags};
use lmdb::Error::NotFound;
use dozer_shared::types::{Field, Operation, Record};
use dozer_shared::types::Field::{Binary, Boolean, Bson, Decimal, Float, Int, Null, Timestamp};
use crate::state::{Aggregator, StateStore, StateStoreError, StateStoreErrorType, StateStoresManager};
use crate::state::lmdb_state::AggrOperation::{Delete, Insert, Update};
use crate::state::lmdb_state::FieldReference::{Ref, Val};
use crate::state::StateStoreErrorType::{AggregatorError, GetOperationError, OpenOrCreateError, SchemaMismatchError, StoreOperationError, TransactionError};

struct LmdbStateStoreManager {
    env: Arc<Environment>
}

impl LmdbStateStoreManager {
    pub fn new(path: &Path, max_size: usize) -> Result<Box<dyn StateStoresManager>, StateStoreError> {

        let res = Environment::new()
            .set_map_size(max_size)
            .set_max_dbs(256)
            .set_max_readers(256)
            .open(path);

        if res.is_err() {
            return Err(StateStoreError::new(OpenOrCreateError, res.err().unwrap().to_string()));
        }
        Ok(Box::new(LmdbStateStoreManager { env: Arc::new(res.unwrap()) }))
    }
}

impl StateStoresManager for LmdbStateStoreManager {

    fn init_state_store<'a> (&'a self, id: String) -> Result<Box<dyn StateStore + 'a>, StateStoreError> {

        let r_db = self.env.create_db(Some(id.as_str()), DatabaseFlags::empty());
        if r_db.is_err() {
            return Err(StateStoreError::new(OpenOrCreateError, r_db.err().unwrap().to_string()));
        }

        let r_tx = self.env.begin_rw_txn();
        if r_tx.is_err() {
            return Err(StateStoreError::new(TransactionError, r_tx.err().unwrap().to_string()));
        }

        return Ok(Box::new(LmdbStateStore::new(
            id, r_db.unwrap(), r_tx.unwrap()
        )));
    }

}


struct LmdbStateStore<'a> {
    id: String,
    db: Database,
    tx: RwTransaction<'a>
}

impl <'a> LmdbStateStore<'a> {
    pub fn new(id: String, db: Database, tx: RwTransaction<'a>) -> Self {
        Self { id, db, tx }
    }
}


macro_rules! db_check {
    ($e:expr) => {
        if $e.is_err() {
            return Err(StateStoreError::new(TransactionError, "put / del / get internal error".to_string()));
        }
    }
}



impl <'a> StateStore for LmdbStateStore<'a> {

    fn checkpoint(&mut self) -> Result<(), StateStoreError> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError> {
        let r = self.tx.put(self.db, &key, &value, WriteFlags::empty());
        if r.is_err() { return Err(StateStoreError::new(StoreOperationError, r.unwrap_err().to_string())); }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
        let r = self.tx.get(self.db, &key);
        if r.is_ok() { return Ok(Some(r.unwrap())); }
        else {
            if r.unwrap_err() == NotFound {
                Ok(None)
            }
            else {
                Err(StateStoreError::new(GetOperationError, r.unwrap_err().to_string()))
            }
        }
    }

}


pub enum FieldRule {
    Dimension(usize),
    Measure(usize, Box<dyn Aggregator>),
    Value(usize)
}


struct SizedAggregationDataset {
    dataset_id: u8,
    out_fields_count: usize,
    out_dimensions: Vec<(usize, usize)>,
    out_measures: Vec<(usize, usize, Box<dyn Aggregator>)>,
    out_values: Vec<(usize, usize)>,
    state_size: usize,
    state_offsets: Vec<usize>
}

#[derive(Clone)]
enum FieldReference<'a> {
    Ref(&'a Field),
    Val(Field)
}

struct HashedRecord<'a> {
    hash: u64,
    fields: Vec<Option<FieldReference<'a>>>
}

impl <'a> HashedRecord<'a> {

    pub fn new(hash: u64, sz: usize) -> Self {
        Self { hash, fields: vec![None; sz] }
    }

    pub fn set_ref(&mut self, idx: usize, f: &'a Field) {
        self.fields[idx] = Some(Ref(f));
    }

    pub fn set_val(& mut self, idx: usize, f: Field) {
        self.fields[idx] = Some(Val(f));
    }
}

pub enum AggrOperation<'a> {
    Delete(HashedRecord<'a>),
    Insert(HashedRecord<'a>),
    Update(HashedRecord<'a>, HashedRecord<'a>)
}

impl SizedAggregationDataset {

    pub fn new(dataset_id: u8, store: &mut dyn StateStore, output_fields: Vec<FieldRule>) -> Result<SizedAggregationDataset, StateStoreError> {

        let out_fields_count = output_fields.len();
        let mut out_measures: Vec<(usize, usize, Box<dyn Aggregator>)> = Vec::new();
        let mut out_dimensions: Vec<(usize, usize)> = Vec::new();
        let mut out_values: Vec<(usize, usize)> = Vec::new();
        let mut ctr = 0;

        for rule in output_fields.into_iter() {
            match rule {
                FieldRule::Measure(idx, aggr) => {
                    out_measures.push((idx, ctr, aggr));
                }
                FieldRule::Dimension(idx) => {
                    out_dimensions.push((idx, ctr));
                }
                FieldRule::Value(idx) => {
                    out_values.push((idx, ctr));
                }
            }
            ctr += 1;
        }

        let mut state_size: usize = 0;
        let mut state_offsets: Vec<usize> = Vec::with_capacity(out_measures.len());

        for ai in &out_measures {
            let sz = ai.2.get_state_size();
            if sz.is_none() {
                return Err(StateStoreError::new(AggregatorError, "Aggregator is not sized".to_string()));
            }
            state_offsets.push(state_size);
            state_size += sz.unwrap();
        }

        store.put(&dataset_id.to_ne_bytes(), &0_u8.to_ne_bytes())?;

        Ok(SizedAggregationDataset {
            dataset_id, out_measures, out_dimensions, out_values, out_fields_count,
            state_size, state_offsets
        })
    }

    fn get_fields_hash(&self, fields: Vec<&Field>) -> Result<u64, StateStoreError> {

        let mut hasher = AHasher::default();
        let mut ctr = 0;

        for f in fields {
            hasher.write_i32(ctr);
            match f {
                Int(i) => { hasher.write_u8(1); hasher.write_i64(*i); }
                Float(f) => { hasher.write_u8(2); hasher.write(&((*f).to_ne_bytes())); }
                Boolean(b) => { hasher.write_u8(3); hasher.write_u8(if *b { 1_u8} else { 0_u8 }); }
                dozer_shared::types::Field::String(s) => { hasher.write_u8(4); hasher.write(s.as_str().as_bytes()); }
                Binary(b) => { hasher.write_u8(5); hasher.write(b.as_ref()); }
                Decimal(d) => { hasher.write_u8(6); hasher.write(&d.serialize()); }
                Timestamp(t) => { hasher.write_u8(7); hasher.write_i64(t.timestamp()) }
                Bson(b) => { hasher.write_u8(8); hasher.write(b.as_ref()); }
                Null => {  hasher.write_u8(0); },
                _ => { return Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid field type".to_string())) }
            }
            ctr += 1;
        }
        Ok(hasher.finish())
    }

    fn get_op_hash(&self, r: &Record) -> Result<u64, StateStoreError> {

        let mut v = Vec::<&Field>::with_capacity(self.out_dimensions.len());
        for i in &self.out_dimensions {
            v.push(&r.values[i.0])
        }
        self.get_fields_hash(v)
    }

    fn prepare_record<'a>(&self, r: &'a Record) -> Result<HashedRecord<'a>, StateStoreError> {

        let mut out_rec = HashedRecord::new(self.get_op_hash(r)?, self.out_fields_count);
        for v in &self.out_values {
            out_rec.set_ref(v.1, &r.values[v.0]);
        }
        for v in &self.out_dimensions {
            out_rec.set_ref(v.1, &r.values[v.0]);
        }

        Ok(out_rec)
    }

    fn get_full_key(&self, hash: u64) -> Vec<u8> {

        let mut vec = Vec::with_capacity(9);
        vec.extend_from_slice(&self.dataset_id.to_ne_bytes());
        vec.extend_from_slice(&hash.to_ne_bytes());
        vec
    }

    fn calc_and_fill_measures<'a>(&self, value: Option<&[u8]>, r: &'a Record, out_rec: &'a mut HashedRecord<'a>, delete: bool) -> Result<Vec<u8>, StateStoreError> {

        let mut new_val = Vec::<u8>::with_capacity(self.state_size);
        let mut offset: usize = 0;
        let mut idx: i32 = 0;

        for measure in &self.out_measures {

            let mut slice = if value.is_some() { Some(&value.unwrap()[offset .. offset + measure.2.get_state_size().unwrap()]) } else { None };
            if delete {
                let r = measure.2.delete(slice, &r.values[measure.0])?;
                out_rec.set_val(measure.1, measure.2.get_value(&r[0..]));
                new_val.extend(r);
            }
            else{
                let r = measure.2.insert( slice, &r.values[measure.0])?;
                out_rec.set_val(measure.1, measure.2.get_value(&r[0..]));
                new_val.extend(r);
            }

            idx += 1;
            offset += measure.2.get_state_size().unwrap();
        }

        Ok(new_val)

    }

    fn aggregate(&self, store: &mut dyn StateStore, op: &Operation) -> Result<Option<Vec<Record>>, StateStoreError> {

        match op {
            Operation::Insert {table_name, new} => {
                let mut out_rec = self.prepare_record(new)?;
                let full_key = self.get_full_key(out_rec.hash);

                let data = store.get(full_key.as_slice())?;
                let curr_state = self.calc_and_fill_measures(
                    store.get(&full_key.as_slice())?, new, &mut out_rec, false
                )?;
                store.put(&full_key[0..], &curr_state[0..]);
                Ok(None)
            }
            Operation::Delete {ref table_name, ref old} => {
                let out_rec = self.prepare_record(old)?;
                Ok(None)

            }
            Operation::Update {ref table_name, ref old, ref new} => {
                let old_out_rec = self.prepare_record(old)?;
                let new_out_rec = self.prepare_record(new)?;
                Ok(None)

            }
            _ => { return Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid operation".to_string())); }
        }

    }


    fn get_accumulated(&self, store: &mut dyn StateStore, key: &[u8]) -> Result<Option<Field>, StateStoreError> {

        // let mut full_key = Vec::<u8>::with_capacity(key.len() + 1);
        // full_key[0] = self.dataset;
        // full_key[1..].copy_from_slice(key);
        //
        // let existing = self.tx.get(&full_key)?;
        // Ok(if existing.is_some() { Some(self.acc.get_value(existing.unwrap())) } else { None })

        Ok(None)
    }
}




mod tests {
    use std::path::Path;
    use bytemuck::{from_bytes, from_bytes_mut};
    use dozer_shared::types::{Field, Operation, Record};
    use crate::state::accumulators::IntegerSumAggregator;
    use crate::state::lmdb_state::{FieldRule, LmdbStateStoreManager, SizedAggregationDataset};
    use rand::Rng; // 0.8.5

    #[test]
    fn test() {

        let sm = LmdbStateStoreManager::new(Path::new("data"), 1024*1024*1024*10);
        let ss = sm.unwrap();
        let mut store = ss.init_state_store("test".to_string()).unwrap();

        let agg = SizedAggregationDataset::new(
            0x02_u8, store.as_mut(),
            vec![
                FieldRule::Dimension(0), // City
                FieldRule::Dimension(1), // Country
                FieldRule::Dimension(2), // Country
                FieldRule::Measure(3, Box::new(IntegerSumAggregator::new())),
                FieldRule::Measure(4, Box::new(IntegerSumAggregator::new())),
                FieldRule::Measure(5, Box::new(IntegerSumAggregator::new())),
                FieldRule::Measure(6, Box::new(IntegerSumAggregator::new())),
                FieldRule::Measure(7, Box::new(IntegerSumAggregator::new())),
                FieldRule::Measure(8, Box::new(IntegerSumAggregator::new()))
            ]
        ).unwrap();



        for i in 0..1000000 {

            let num = rand::thread_rng().gen_range(0..100000);

            let op = Operation::Insert {
                table_name: "test".to_string(),
                new: Record::new(0, vec![
                    Field::String(format!("Milan{}", 1).to_string()),
                    Field::String("Italy".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20)
                ])
            };

            agg.aggregate(store.as_mut(), &op);
        }



        println!("ciao")


    }



}

