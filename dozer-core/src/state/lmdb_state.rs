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
use crate::state::lmdb_state::AggrParams::{Delete, Insert, Update};
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
        else { return Err(StateStoreError::new(GetOperationError, r.unwrap_err().to_string())) }
    }

}

pub struct AggregationRule {
    index: u32,
    aggr: Box<dyn Aggregator>
}

impl AggregationRule {
    pub fn new(index: u32, aggr: Box<dyn Aggregator>) -> Self {
        Self { index, aggr }
    }
}

struct SizedAggregationDataset {
    dataset_id: u8,
    dimensions: Vec<Vec<u32>>,
    measures: Vec<AggregationRule>,
    state_size: usize,
    state_offsets: Vec<usize>
}

pub enum AggrParams<'a> {
    Delete { hash: u64, dims: Vec<&'a Field>, vals: Vec<&'a Field> },
    Insert { hash: u64, dims: Vec<&'a Field>, vals: Vec<&'a Field> },
    Update { old_hash: u64, old_dims: Vec<&'a Field>, old_vals: Vec<&'a Field>, new_hash: u64, new_dims: Vec<&'a Field>, new_vals: Vec<&'a Field> }
}

impl SizedAggregationDataset {

    pub fn new(dataset_id: u8, store: &mut dyn StateStore, dimensions: Vec<Vec<u32>>, measures: Vec<AggregationRule>) -> Result<SizedAggregationDataset, StateStoreError> {

        let mut state_size: usize = 0;
        let mut state_offsets: Vec<usize> = Vec::with_capacity(measures.len());

        for ai in &measures {
            let sz = ai.aggr.get_state_size();
            if sz.is_none() {
                return Err(StateStoreError::new(AggregatorError, "Aggregator is not sized".to_string()));
            }
            state_offsets.push(state_size);
            state_size += sz.unwrap();
        }

        store.put(&dataset_id.to_ne_bytes(), &0_u8.to_ne_bytes())?;

        Ok(SizedAggregationDataset {
            dataset_id, dimensions, measures,
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

    fn get_op_hash(&self, r: &Record, fields_idx: &Vec<u32>) -> Result<u64, StateStoreError> {

        let mut v = Vec::<&Field>::with_capacity(fields_idx.len());
        for i in fields_idx {
            v.push(&r.values[(*i) as usize])
        }
        self.get_fields_hash(v)
    }

    fn get_measure_vals<'a>(&self, r: &'a Record, aggrs: &Vec<AggregationRule>) -> Vec<&'a Field> {

        let mut v = Vec::<&Field>::new();
        for aggr in aggrs {
            v.push(&r.values[aggr.index as usize])
        }
        v

    }

    fn get_op_hashes<'a>(&self, op: &'a Operation, fields_idx: &Vec<Vec<u32>>, aggrs: &Vec<AggregationRule>, v: &'a mut Vec<AggrParams<'a>>) -> Result<(), StateStoreError> {

        match *op {
            Operation::Insert {ref table_name, ref new} => {
                for idx in fields_idx {
                    let hash = self.get_op_hash(new, idx)?;
                    v.push(AggrParams::Insert {hash: hash, vals: self.get_measure_vals(new, aggrs) });
                }
            }
            Operation::Delete {ref table_name, ref old} => {
                for idx in fields_idx {
                    let hash = self.get_op_hash(old, idx)?;
                    v.push(AggrParams::Delete {hash: hash, vals: self.get_measure_vals(old, aggrs) });
                }
            }
            Operation::Update {ref table_name, ref old, ref new} => {
                for idx in fields_idx {
                    let old_hash = self.get_op_hash(old, idx)?;
                    let new_hash = self.get_op_hash(new, idx)?;
                    v.push(AggrParams::Update {
                        old_hash, new_hash,
                        old_vals: self.get_measure_vals(old, aggrs),
                        new_vals: self.get_measure_vals(new, aggrs)
                    });
                }
            }
            _ => { return Err(StateStoreError::new(StateStoreErrorType::AggregatorError, "Invalid operation".to_string())); }
        };

        Ok(())
    }

    fn compose_full_key(&self, hash: u64) -> Vec<u8> {

        let mut vec = Vec::with_capacity(9);
        vec.extend_from_slice(&self.dataset_id.to_ne_bytes());
        vec.extend_from_slice(&hash.to_ne_bytes());
        vec
    }

    fn execute_aggregation(
        &self, store: &mut dyn StateStore, key: Vec<u8>, measures: &Vec<AggregationRule>,
        del_vals: Option<Vec<&Field>>,
        add_vals: Option<Vec<&Field>>) -> Result<Option<Vec<Operation>>, StateStoreError> {

        let value = store.get(&key.as_slice())?;
        let mut new_value = if value.is_none() { Vec::<u8>::with_capacity(self.state_size) } else { Vec::<u8>::from(value.unwrap()); };
        let mut offset: usize = 0;
        let mut idx: i32 = 0;

        let mut final_vals = Vec::<Operation>::new();

        for measure in measures {

            let slice = new_value.slice(offset, offset + measure.aggr.get_state_size());
            if del_vals.is_some() {
                measure.aggr.delete(slice, del_vals[idx])
            }
            if add_vals.is_some() {
                measure.aggr.insert(slice, add_vals[idx])
            }

            final_vals.push(measure.aggr.get_value(slice));


            idx += 1;
            offset += measure.aggr.get_state_size();
        }

        Ok(if get_values {Some(final_vals)} else { None })
    }

    fn aggregate(&self, store: &mut dyn StateStore, op: &Operation, retrieve: bool) -> Result<Option<Vec<Record>>, StateStoreError> {

        let mut aggr_params : Vec<AggrParams> = Vec::with_capacity(self.dimensions.len());
        self.get_op_hashes(op, &self.dimensions, &self.measures, &mut aggr_params);

        let fields: Vec<Record>


        for aggr_param in aggr_params {
            match aggr_param {
                Delete { hash, vals } => {
                    let full_key = self.compose_full_key(hash);
                    self.execute_aggregation(store, full_key, &self.measures, retrieve, Some(vals), None)
                },
                Insert { hash, vals } => {
                    let full_key = self.compose_full_key(hash);
                    self.execute_aggregation(store, full_key, &self.measures, retrieve, None, Some(vals))
                },
                Update { old_hash, old_vals, new_hash, new_vals} => {
                    if old_hash == new_hash {
                        let full_key = self.compose_full_key(old_hash);
                        self.execute_aggregation(store, full_key, &self.measures, retrieve, Some(old_vals), Some(new_vals))
                    }
                    else {
                        let old_full_key = self.compose_full_key(old_hash);
                        self.execute_aggregation(store, old_full_key, &self.measures, retrieve, Some(old_vals), None);
                        let new_full_key = self.compose_full_key(new_hash);
                        self.execute_aggregation(store, new_full_key, &self.measures, retrieve, None, Some(new_vals));
                    }
                }
            }
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
    use crate::state::lmdb_state::{AggregationRule, LmdbStateStoreManager, SizedAggregationDataset};

    #[test]
    fn test() {

        let sm = LmdbStateStoreManager::new(Path::new("data"), 1024*1024*1024*10);
        let ss = sm.unwrap();
        let mut store = ss.init_state_store("test".to_string()).unwrap();

        let agg = SizedAggregationDataset::new(
          0x02_u8, store.as_mut(),
            vec![vec![0], vec![1], vec![0,1]],
            vec![AggregationRule::new(2, Box::new(IntegerSumAggregator::new()))]
        ).unwrap();

        let op = Operation::Insert {
            table_name: "test".to_string(),
            new: Record::new(0, vec![
                Field::String("M".to_string()),
                Field::Int(40),
                Field::Int(10)
            ])
        };

        agg.aggregate(store.as_mut(), &op, true);

        println!("ciao")


    }



}

