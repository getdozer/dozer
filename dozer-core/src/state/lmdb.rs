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
use dozer_types::types::{Field, Operation, Record};
use dozer_types::types::Field::{Binary, Boolean, Bson, Decimal, Float, Int, Null, Timestamp};
use crate::state::{StateStore, StateStoreError, StateStoreErrorType, StateStoresManager};
use crate::state::StateStoreErrorType::{AggregatorError, GetOperationError, OpenOrCreateError, SchemaMismatchError, StoreOperationError, TransactionError};

pub struct LmdbStateStoreManager {
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



