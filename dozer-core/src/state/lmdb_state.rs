use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hasher;
use std::mem::{size_of, size_of_val};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use ahash::AHasher;
use lmdb::{Database, DatabaseFlags, Environment, Error, RwTransaction, Transaction, WriteFlags};
use lmdb::Error::NotFound;
use dozer_shared::types::{Field, Operation, Record};
use dozer_shared::types::Field::{Binary, Boolean, Bson, Decimal, Float, Int, Null, Timestamp};
use crate::state::{Aggregator, StateStore, StateStoreError, StateStoreErrorType, StateStoresManager};
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

    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError> {
        let r = self.tx.del(self.db, &key, None);
        if r.is_err() { return Err(StateStoreError::new(StoreOperationError, r.unwrap_err().to_string())); }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
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

pub struct MemoryStateStore {
    data: HashMap<Vec<u8>,Vec<u8>>
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }
}

impl StateStore for MemoryStateStore {

    fn checkpoint(&mut self) -> Result<(), StateStoreError> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError> {
        self.data.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
        let r = self.data.get(key);
        Ok(if r.is_none() { None } else { Some(r.unwrap().as_slice()) })
    }

    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError> {
        self.data.remove(key);
        Ok(())
    }
}


pub enum FieldRule {
    Dimension(usize),
    Measure(Box<dyn Aggregator>)
}


struct SizedAggregationDataset {
    dataset_id: u16,
    output_schema_id: u64,
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
        output_schema_id: u64, output_fields: Vec<FieldRule>) -> Result<SizedAggregationDataset, StateStoreError> {

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

    fn get_record_hash(&self, r: &Record) -> Result<u64, StateStoreError> {

        let mut hasher = AHasher::default();
        let mut ctr = 0;

        for dimension in &self.out_dimensions {
            hasher.write_i32(ctr);
            match &r.values[dimension.0] {
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

    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) {

        for v in &self.out_dimensions {
            out_rec.values[v.1] = in_rec.values[v.0].clone();
        }
    }

    fn get_record_key(&self, hash: u64, database_id: u16) -> Result<Vec<u8>, StateStoreError> {
        let mut vec = Vec::with_capacity(size_of_val(&hash) + size_of_val(&database_id));
        vec.extend_from_slice(&database_id.to_ne_bytes());
        vec.extend_from_slice(&hash.to_ne_bytes());
        Ok(vec)
    }

    fn calc_and_fill_measures(
        &self, curr_state: Option<&[u8]>, deleted_record: Option<&Record>, inserted_record: Option<&Record>,
        out_rec_delete: &mut Record, out_rec_insert: &mut Record,
        op: AggregatorOperation) -> Result<Vec<u8>, StateStoreError> {

        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.out_measures {

            let mut curr_state_slice = if curr_state.is_none() { None } else {
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
            offset += (next_state_slice.len() + 2);

            if (next_state_slice.len() > 0) {
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

    fn update_segment_count(&self, store: &mut dyn StateStore, key: Vec<u8>, delta: u64, decr: bool) -> Result<u64, StateStoreError> {

        let bytes = store.get(key.as_slice())?;
        let curr_count = if bytes.is_some() { u64::from_ne_bytes(bytes.unwrap().try_into().unwrap()) } else {0_u64};
        store.put(key.as_slice(), (if decr {curr_count - delta} else {curr_count + delta}).to_ne_bytes().as_slice());
        Ok(curr_count)
    }


    fn aggregate(&self, store: &mut dyn StateStore, op: Operation) -> Result<Vec<Operation>, StateStoreError> {

        match op {
            Operation::Insert {table_name, new} => {

                let mut out_rec_insert = Record::nulls(self.output_schema_id, self.out_fields_count);
                let mut out_rec_delete = Record::nulls(self.output_schema_id, self.out_fields_count);
                let record_hash = self.get_record_hash(&new)?;
                let record_key = self.get_record_key(record_hash, self.dataset_id)?;

                let record_count_key = self.get_record_key(record_hash, self.dataset_id + 1)?;
                let prev_count = self.update_segment_count(store, record_count_key, 1, false)?;

                let curr_state = store.get(record_key.as_slice())?;
                let new_state = self.calc_and_fill_measures(
                    curr_state, None, Some(&new),
                    &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Insert
                )?;

                let res = vec![
                    if curr_state.is_none() {
                        self.fill_dimensions(&new, &mut out_rec_insert);
                        Operation::Insert {table_name: "".to_string(), new: out_rec_insert}
                    }
                    else {
                        self.fill_dimensions(&new, &mut out_rec_insert);
                        self.fill_dimensions(&new, &mut out_rec_delete);
                        Operation::Update {table_name: "".to_string(), new: out_rec_insert, old: out_rec_delete}
                    }
                ];

                store.put(record_key.as_slice(), new_state.as_slice())?;

                Ok(res)
            }
            Operation::Delete {ref table_name, ref old} => {

                let mut out_rec_insert = Record::nulls(self.output_schema_id, self.out_fields_count);
                let mut out_rec_delete = Record::nulls(self.output_schema_id, self.out_fields_count);
                let record_hash = self.get_record_hash(&old)?;
                let record_key = self.get_record_key(record_hash, self.dataset_id)?;

                let record_count_key = self.get_record_key(record_hash, self.dataset_id + 1)?;
                let prev_count = self.update_segment_count(store, record_count_key, 1, true)?;

                let curr_state = store.get(record_key.as_slice())?;
                let new_state = self.calc_and_fill_measures(
                    curr_state, Some(&old), None,
                    &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Delete
                )?;

                let res = vec![
                    if prev_count == 0 {
                        self.fill_dimensions(&old, &mut out_rec_delete);
                        Operation::Delete {table_name: "".to_string(), old: out_rec_delete}
                    }
                    else {
                        self.fill_dimensions(&old, &mut out_rec_insert);
                        self.fill_dimensions(&old, &mut out_rec_delete);
                        Operation::Update {table_name: "".to_string(), new: out_rec_insert, old: out_rec_delete}
                    }
                ];

                if prev_count > 0 {
                    store.put(record_key.as_slice(), new_state.as_slice())?;
                }
                else {
                    store.del(record_key.as_slice())?
                }
                Ok(res)

            }
            Operation::Update {ref table_name, ref old, ref new} => {

                let old_record_hash = self.get_record_hash(&old)?;
                let new_record_hash = self.get_record_hash(&new)?;

                if (old_record_hash == new_record_hash) {
                    // Dimension values were not changed

                    let mut out_rec_insert = Record::nulls(self.output_schema_id, self.out_fields_count);
                    let mut out_rec_delete = Record::nulls(self.output_schema_id, self.out_fields_count);
                    let record_key = self.get_record_key(old_record_hash, self.dataset_id)?;

                    let curr_state = store.get(record_key.as_slice())?;
                    let new_state = self.calc_and_fill_measures(
                        curr_state, Some(&old), Some(&new),
                        &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Update
                    )?;

                    self.fill_dimensions(&new, &mut out_rec_insert);
                    self.fill_dimensions(&old, &mut out_rec_delete);

                    let res = vec![
                        Operation::Update {table_name: "".to_string(), new: out_rec_insert, old: out_rec_delete}
                    ];

                    store.put(record_key.as_slice(), new_state.as_slice())?;

                    Ok(res)

                }
                else {
                    Ok(vec![])
                    // Dimension values changed

                }


                //Ok(vec![])

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

    use std::collections::HashMap;
    use std::path::Path;
    use bytemuck::{from_bytes, from_bytes_mut};
    use dozer_shared::types::{Field, Operation, Record};
    use crate::state::accumulators::IntegerSumAggregator;
    use crate::state::lmdb_state::{FieldRule, LmdbStateStoreManager, MemoryStateStore, SizedAggregationDataset};
    use rand::Rng;
    use crate::state::StateStore;


    #[test]
    fn test_insert() {

        let mut store = MemoryStateStore::new();

        let agg = SizedAggregationDataset::new(
            0x02_u16, &mut store, 0,
            vec![
                FieldRule::Dimension(0), // City
                FieldRule::Dimension(1), // Country
                FieldRule::Measure(Box::new(IntegerSumAggregator::new(3))) // People
            ]
        ).unwrap();

        // Insert 10
        let i = Operation::Insert {
            table_name: "test".to_string(),
            new: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Insert {
            table_name: "".to_string(),
            new: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        });

        // Insert another 10
        let i = Operation::Insert {
            table_name: "test".to_string(),
            new: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Update {
            table_name: "".to_string(),
            old: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(20)
            ])
        });

        // update from 10 to 5
        let i = Operation::Update {
            table_name: "".to_string(),
            old: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(5)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Update {
            table_name: "".to_string(),
            old: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(20)
            ]),
            new: Record::new(0, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(15)
            ])
        });



        println!("ciao")





    }


    #[test]
    fn test() {

        let sm = LmdbStateStoreManager::new(Path::new("data"), 1024*1024*1024*10);
        let ss = sm.unwrap();
        let mut store = ss.init_state_store("test".to_string()).unwrap();

        let agg = SizedAggregationDataset::new(
            0x02_u16, store.as_mut(), 100,
            vec![
                FieldRule::Dimension(0), // City
                FieldRule::Dimension(1), // Region
                FieldRule::Dimension(2), // Country
                FieldRule::Measure(Box::new(IntegerSumAggregator::new(3)))
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

            let v = agg.aggregate(store.as_mut(), op);
         //   println!("ciao")

        }



    }



}

