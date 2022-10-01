use std::path::Path;
use std::sync::Arc;
use std::{fs, thread};
use std::time::Duration;
use crossbeam::channel::bounded;
use futures::{AsyncReadExt, SinkExt};
use crate::state::{StateStore, StateStoreError, StateStoresManager};
use crate::state::lmdb_sys::{Database, DatabaseOptions, Environment, EnvOptions, Transaction};
use crate::state::StateStoreErrorType::{GetOperationError, InvalidPath, OpenOrCreateError, StoreOperationError, TransactionError};

pub struct LmdbStateStoreManager {
    path: String,
    max_size: usize
}

impl LmdbStateStoreManager {
    pub fn new(path: String, max_size: usize) -> Result<Arc<dyn StateStoresManager>, StateStoreError> {

        fs::create_dir(path.clone());
        Ok(Arc::new(LmdbStateStoreManager {path: path.clone() , max_size }))
    }
}

impl StateStoresManager for LmdbStateStoreManager {

     fn init_state_store (&self, id: String) -> Result<Box<dyn StateStore>, StateStoreError> {

         let full_path = Path::new(&self.path).join(&id);
         let r = fs::create_dir(&full_path);
         if r.is_err() {
             return Err(StateStoreError::new(InvalidPath, "Unable to create path".to_string()));
         }

         let mut env_opt = EnvOptions::default();
         env_opt.no_sync = true;
         env_opt.max_dbs = Some(10);
         env_opt.map_size = Some(self.max_size);

         let env = Arc::new(Environment::new(full_path.to_str().unwrap().to_string(), Some(env_opt)).unwrap());
         let mut tx = Transaction::begin(env.clone()).unwrap();
         let db = Database::open(env.clone(), &tx, id.to_string(), Some(DatabaseOptions::default()) ).unwrap();

         Ok(Box::new(LmdbStateStore { env: env.clone(), tx, db}))

    }

}


struct LmdbStateStore {
    env: Arc<Environment>,
    db: Database,
    tx: Transaction
}


impl StateStore for LmdbStateStore {

    fn checkpoint(&mut self) -> Result<(), StateStoreError> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateStoreError> {
        let r = self.db.put(&self.tx, &key, &value, None);
        if r.is_err() { return Err(StateStoreError::new(StoreOperationError, "Error during put operation".to_string())) }
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> Result<(), StateStoreError> {
         let r = self.db.del(&self.tx, &key, None);
         if r.is_err() { return Err(StateStoreError::new(StoreOperationError, "Error during del operation".to_string())); }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, StateStoreError> {
        let r = self.db.get(&   self.tx, &key);
        if r.is_ok() {
            Ok(r.unwrap())
        }
        else {
            Err(StateStoreError::new(GetOperationError, "Error during get operation".to_string()))
        }
    }

}

#[test]
fn test_mt_lmdb_store() {

    let sm = LmdbStateStoreManager::new("./data".to_string(), 1024*1024*1024*5).unwrap();

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



