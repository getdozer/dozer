use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use lmdb::{Database, DatabaseFlags, Environment, Error, RwTransaction, Transaction, WriteFlags};
use lmdb::Error::NotFound;
use dozer_shared::types::Field;
use crate::state::{AccumulationDataset, Accumulator, StateStore, StateStoreError, StateStoresManager};
use crate::state::StateStoreErrorType::{AccumulationError, GetOperationError, OpenOrCreateError, SchemaMismatchError, StoreOperationError, TransactionError};

// struct LmdbStateStoreManager {
//     env: Arc<Environment>
// }
//
// impl LmdbStateStoreManager {
//     pub fn new(path: &Path, max_size: usize) -> Result<Box<dyn StateStoresManager>, StateStoreError> {
//
//         let res = Environment::new()
//             .set_map_size(max_size)
//             .set_max_dbs(256)
//             .set_max_readers(256)
//             .open(path);
//
//         if res.is_err() {
//             return Err(StateStoreError::new(OpenOrCreateError, res.err().unwrap().to_string()));
//         }
//         Ok(Box::new(LmdbStateStoreManager { env: Arc::new(res.unwrap()) }))
//     }
// }
//
// impl StateStoresManager for LmdbStateStoreManager {
//
//     fn init_state_store<'a>(&'a self, id: String) -> Result<Box<dyn StateStore + 'a>, StateStoreError> {
//
//         let r_tx = self.env.begin_rw_txn();
//         if r_tx.is_err() {
//             return Err(StateStoreError::new(TransactionError, r_tx.err().unwrap().to_string()));
//         }
//
//         let r_db = self.env.create_db(Some(id.as_str()), DatabaseFlags::empty());
//         if r_db.is_err() {
//             return Err(StateStoreError::new(OpenOrCreateError, r_db.err().unwrap().to_string()));
//         }
//
//         return Ok(Box::new(LmdbStateStore::new(
//             id, self.env.clone(),
//             LmdbTransaction::new(r_db.unwrap(), r_tx.unwrap())
//         )));
//     }
//
// }
//

struct Db<'a> {
    tx: &'a mut RwTransaction<'a>
}

struct DbManager<'a> {
    tx: RwTransaction<'a>
}

impl <'a> GenericDbManager<'a> for DbManager<'a> {

    fn create_tx(&'a mut self) -> Box<Db<'a>> {
        Box::new(Db {tx : &mut self.tx})
    }
}

trait GenericDbManager<'a> {
    fn create_tx(&'a mut self) -> Box<Db<'a>>;
}



struct LmdbStateStore<'a> {
    id: String,
    env: Arc<Environment>,
    tx: LmdbTransaction<'a>
}

impl <'a>LmdbStateStore<'a> {

    pub fn new(id: String, env: Arc<Environment>, tx: LmdbTransaction<'a>) -> Self {
        Self { id, env, tx :tx}
    }
}


macro_rules! db_check {
    ($e:expr) => {
        if $e.is_err() {
            return Err(StateStoreError::new(TransactionError, "put / del / get internal error".to_string()));
        }
    }
}



impl <'a> StateStore<'a> for LmdbStateStore<'a> {

    fn checkpoint(&mut self) -> Result<(), StateStoreError> {
        todo!()
    }

    fn init_accumulation_dataset(&'a mut self, dataset: u8, acc: Box<dyn Accumulator>) -> Result<Box<dyn AccumulationDataset<'a>>, StateStoreError> {

        let prev_type_ids = self.tx.get(&dataset.to_ne_bytes())?;
        if prev_type_ids.is_some() {
            if prev_type_ids.unwrap()[0] != acc.get_type() {
                return Err(StateStoreError::new(SchemaMismatchError, "Older and newer schema for accumulation set do not match".to_string()));
            }
        }
        self.tx.put(&dataset.to_ne_bytes(), &acc.get_type().to_ne_bytes())?;
        Ok(Box::new(LmdbAccumulationDataset::new(dataset, &mut self.tx, acc) ))

    }


}


struct LmdbTransaction<'a> {
    db: Database,
    tx: RwTransaction<'a>
}

impl<'a> LmdbTransaction<'a> {

    pub fn new(db: Database, tx: RwTransaction<'a>) -> Self {
        Self { db, tx }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError> {
        let r = self.tx.put(self.db, &key, &value, WriteFlags::empty());
        if r.is_err() { return Err(StateStoreError::new(StoreOperationError, r.unwrap_err().to_string())); }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
        let r = self.tx.get(self.db, &key);
        if r.is_ok() { return Ok(Some(r.unwrap())); }
        else { return Err(StateStoreError::new(GetOperationError, r.unwrap_err().to_string())) }
    }
}


struct LmdbAccumulationDataset<'a> {
    dataset: u8,
    tx: &'a mut LmdbTransaction<'a>,
    acc: Box<dyn Accumulator>
}

impl<'a> LmdbAccumulationDataset<'a> {
    pub fn new(dataset: u8, tx: &'a mut LmdbTransaction<'a>, acc: Box<dyn Accumulator>) -> Self {
        Self { dataset, tx, acc }
    }
}

impl <'a> AccumulationDataset<'a> for LmdbAccumulationDataset<'a> {

    fn accumulate(&mut self, key: &[u8], value: Field, retrieve: bool) -> Result<Option<Field>, StateStoreError> {

        let mut full_key = Vec::<u8>::with_capacity(key.len() + 1);
        full_key[0] = self.dataset;
        full_key[1..].copy_from_slice(key);

        let existing = self.tx.get(&full_key)?;
        let r = self.acc.accumulate(existing, value)?;
        self.tx.put(&full_key, &r)?;

        Ok(if retrieve { Some(self.acc.get_value(&r)) } else { None })
    }

    fn get_accumulated(&self, key: &[u8]) -> Result<Option<Field>, StateStoreError> {

        let mut full_key = Vec::<u8>::with_capacity(key.len() + 1);
        full_key[0] = self.dataset;
        full_key[1..].copy_from_slice(key);

        let existing = self.tx.get(&full_key)?;
        Ok(if existing.is_some() { Some(self.acc.get_value(existing.unwrap())) } else { None })
    }
}


mod tests {




}

