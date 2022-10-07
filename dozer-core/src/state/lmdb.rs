use std::path::Path;
use std::sync::Arc;
use std::{fs, thread};
use std::time::Duration;
use crossbeam::channel::bounded;
use crate::state::{StateStore, StateStoresManager};
use crate::state::lmdb_sys::{Database, DatabaseOptions, Environment, EnvOptions, LmdbError, Transaction};

pub struct LmdbStateStoreManager {
    path: String,
    max_size: usize,
    commit_threshold: u32
}

impl LmdbStateStoreManager {
    pub fn new(path: String, max_size: usize, commit_threshold: u32) -> anyhow::Result<Arc<dyn StateStoresManager>> {

        fs::create_dir(path.clone());
        Ok(Arc::new(LmdbStateStoreManager {path: path.clone() , max_size, commit_threshold }))
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
         env_opt.writable_mem_map = true;

         let env = Arc::new(Environment::new(full_path.to_str().unwrap().to_string(), Some(env_opt))?);
         let mut tx = Transaction::begin(env.clone())?;

         let mut db_opt = DatabaseOptions::default();
         let db = Database::open(env.clone(), &tx, id.to_string(), Some(db_opt) )?;

         Ok(Box::new(LmdbStateStore {
             env: env.clone(), tx, db, commit_counter: 0,
             commit_threshold: self.commit_threshold
         }))
    }
}


struct LmdbStateStore {
    env: Arc<Environment>,
    db: Database,
    tx: Transaction,
    commit_counter: u32,
    commit_threshold: u32
}

impl LmdbStateStore {

    fn renew_tx(&mut self) -> Result<(), LmdbError>{

        self.commit_counter +=1;
        if self.commit_counter >= self.commit_threshold {
            self.tx.commit()?;
            self.tx = Transaction::begin(self.env.clone())?;
            self.commit_counter = 0;
        }
        Ok(())
    }
}


impl StateStore for LmdbStateStore {

    fn checkpoint(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.renew_tx()?;
        self.db.put(&self.tx, &key, &value, None)?;
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.renew_tx()?;
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

    let sm = LmdbStateStoreManager::new(
        "./data".to_string(),
        1024*1024*1024*5,
        20_000
    ).unwrap();

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



