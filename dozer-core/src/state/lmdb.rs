use std::path::Path;
use std::sync::Arc;
use std::{fs, thread};
use std::time::Duration;
use crossbeam::channel::bounded;
use futures::{AsyncReadExt, SinkExt};
use lmdb::{Database, DatabaseFlags, Environment, RwTransaction, Transaction, WriteFlags};
use lmdb::Error::NotFound;
use crate::state::{StateStore, StateStoreError, StateStoresManager};
use crate::state::StateStoreErrorType::{GetOperationError, OpenOrCreateError, StoreOperationError, TransactionError};

pub struct LmdbStateStoreManager {
    env: Arc<Environment>
}

impl LmdbStateStoreManager {
    pub fn new(path: &Path, max_size: usize) -> Result<Arc<dyn StateStoresManager>, StateStoreError> {

        fs::create_dir(path);

        let res = Environment::new()
            .set_map_size(max_size)
            .set_max_dbs(256)
            .set_max_readers(256)
            .open(path);

        if res.is_err() {
            return Err(StateStoreError::new(OpenOrCreateError, res.err().unwrap().to_string()));
        }
        Ok(Arc::new(LmdbStateStoreManager { env: Arc::new(res.unwrap()) }))
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

#[test]
fn test_mt_lmdb_store() {

    let sm = LmdbStateStoreManager::new(Path::new("./data"), 1024*1024*1024*5).unwrap();

    let (mut tx1, mut rx1) = bounded::<i32>(1000);
    let sm_t1 = sm.clone();
    let h1 = thread::spawn(move || {
        let ss1 = sm_t1.init_state_store("test1".to_string());
        rx1.recv();
    });

    let (mut tx2, mut rx2) = bounded(1000);
    let sm_t2 = sm.clone();
    let h2 = thread::spawn(move || {
        let ss2 = sm_t2.init_state_store("test2".to_string());
        rx2.recv();
    });

    thread::sleep(Duration::from_secs(2));
    tx1.send(1);
    tx2.send(2);

    h1.join();
    h2.join();


}



