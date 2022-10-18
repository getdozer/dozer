use crate::state::lmdb_sys::{
    Cursor, Database, DatabaseOptions, EnvOptions, Environment, LmdbError, Transaction,
};
use crate::state::{StateStore, StateStoreCursor, StateStoreOptions, StateStoresManager};
use anyhow::{anyhow, Context};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub struct LmdbStateStoreManager {
    path: String,
    max_size: usize,
    commit_threshold: u32,
}

impl LmdbStateStoreManager {
    pub fn new(path: String, max_size: usize, commit_threshold: u32) -> Self {
        Self {
            path,
            max_size,
            commit_threshold,
        }
    }
}

impl StateStoresManager for LmdbStateStoreManager {
    fn init_state_store(
        &self,
        id: String,
        options: StateStoreOptions,
    ) -> anyhow::Result<Box<dyn StateStore>> {
        let full_path = Path::new(&self.path).join(&id);
        fs::create_dir(&full_path)?;

        let mut env_opt = EnvOptions::default();
        env_opt.no_sync = true;
        env_opt.max_dbs = Some(10);
        env_opt.map_size = Some(self.max_size);
        env_opt.writable_mem_map = true;

        let env = Arc::new(Environment::new(
            full_path.to_str().unwrap().to_string(),
            Some(env_opt),
        )?);
        let tx = Transaction::begin(env.clone())?;

        let mut db_opt = DatabaseOptions::default();
        db_opt.allow_duplicate_keys = options.allow_duplicate_keys;
        let db = Database::open(env.clone(), &tx, id, Some(db_opt))?;

        Ok(Box::new(LmdbStateStore {
            env,
            tx,
            db,
            commit_counter: 0,
            commit_threshold: self.commit_threshold,
        }))
    }
}

struct LmdbStateStore {
    env: Arc<Environment>,
    db: Database,
    tx: Transaction,
    commit_counter: u32,
    commit_threshold: u32,
}

impl LmdbStateStore {
    fn renew_tx(&mut self) -> Result<(), LmdbError> {
        self.commit_counter += 1;
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
        self.db.put(&self.tx, key, value, None)?;
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.renew_tx()?;
        self.db.del(&self.tx, key, None)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<&[u8]>> {
        let r = self.db.get(&self.tx, key)?;
        Ok(r)
    }

    fn cursor(&mut self) -> anyhow::Result<Box<dyn StateStoreCursor>> {
        let cursor = self.db.open_cursor(&self.tx)?;
        Ok(Box::new(LmdbStateStoreCursor { cursor }))
    }
}

pub struct LmdbStateStoreCursor {
    cursor: Cursor,
}

impl StateStoreCursor for LmdbStateStoreCursor {
    fn seek(&mut self, key: &[u8]) -> anyhow::Result<bool> {
        self.cursor
            .seek(key)
            .map_err(|e| anyhow!("{}: {}", e.err_no, e.err_str))
    }

    fn next(&mut self) -> anyhow::Result<bool> {
        self.cursor
            .next()
            .map_err(|e| anyhow!("{}: {}", e.err_no, e.err_str))
    }

    fn prev(&mut self) -> anyhow::Result<bool> {
        self.cursor
            .prev()
            .map_err(|e| anyhow!("{}: {}", e.err_no, e.err_str))
    }

    fn read(&mut self) -> anyhow::Result<Option<(&[u8], &[u8])>> {
        self.cursor
            .read()
            .map_err(|e| anyhow!("{}: {}", e.err_no, e.err_str))
    }
}
