use std::path::Path;
use std::sync::Arc;
use std::{fs, thread};
use std::time::Duration;
use crossbeam::channel::bounded;
use crate::state::{StateStore, StateStoresManager};
use crate::state::lmdb_sys::{Database, DatabaseOptions, Environment, EnvOptions, Transaction};

pub struct LmdbStateStoreManager {
    path: String,
    max_size: usize
}

impl LmdbStateStoreManager {
    pub fn new(path: String, max_size: usize) -> anyhow::Result<Arc<dyn StateStoresManager>> {

        fs::create_dir(path.clone());
        Ok(Arc::new(LmdbStateStoreManager {path: path.clone() , max_size }))
    }
}

impl StateStoresManager for LmdbStateStoreManager {

     fn init_state_store (&self, id: String) -> anyhow::Result<Box<dyn StateStore>> {

         let full_path = Path::new(&self.path).join(&id);
         fs::create_dir(&full_path)?;

         let mut env_opt = EnvOptions::default();
         env_opt.no_sync = true;
         env_opt.max_dbs = Some(10);
         env_opt.map_size = Some(self.max_size);

         let env = Arc::new(Environment::new(full_path.to_str().unwrap().to_string(), Some(env_opt))?);
         let mut tx = Transaction::begin(env.clone())?;
         let db = Database::open(env.clone(), &tx, id.to_string(), Some(DatabaseOptions::default()) )?;

         Ok(Box::new(LmdbStateStore { env: env.clone(), tx, db}))

    }

}


struct LmdbStateStore {
    env: Arc<Environment>,
    db: Database,
    tx: Transaction
}


impl StateStore for LmdbStateStore {

    fn checkpoint(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db.put(&self.tx, &key, &value, None)?;
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.db.del(&self.tx, &key, None)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<&[u8]>> {
        let r = self.db.get(&   self.tx, &key)?;
        Ok(r)
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



