use crate::state::lmdb_sys::{
    Cursor, Database, DatabaseOptions, EnvOptions, Environment, LmdbError, Transaction,
};
use dozer_types::core::state::{
    StateStore, StateStoreCursor, StateStoreOptions, StateStoresManager,
};
use dozer_types::errors::database::DatabaseError;
use dozer_types::errors::database::DatabaseError::{InternalError, OpenOrCreateError};
use dozer_types::internal_err;
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
    ) -> Result<Box<dyn StateStore>, DatabaseError> {
        let full_path = Path::new(&self.path).join(&id);
        fs::create_dir(&full_path)
            .map_err(|_e| OpenOrCreateError(full_path.to_str().unwrap().to_string()))?;

        let mut env_opt = EnvOptions::default();
        env_opt.no_sync = true;
        env_opt.max_dbs = Some(10);
        env_opt.map_size = Some(self.max_size);
        env_opt.writable_mem_map = true;

        let env = Arc::new(internal_err!(Environment::new(
            full_path.to_str().unwrap().to_string(),
            Some(env_opt)
        ))?);
        let tx = internal_err!(Transaction::begin(&env, false))?;

        let mut db_opt = DatabaseOptions::default();
        db_opt.allow_duplicate_keys = options.allow_duplicate_keys;
        let db = internal_err!(Database::open(&env, &tx, id, Some(db_opt)))?;

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
            self.tx = Transaction::begin(&self.env, false)?;
            self.commit_counter = 0;
        }
        Ok(())
    }
}

impl StateStore for LmdbStateStore {
    fn checkpoint(&mut self) -> Result<(), DatabaseError> {
        todo!()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        internal_err!(self.tx.put(&self.db, key, value, None))?;
        Ok(())
    }

    fn del(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), DatabaseError> {
        internal_err!(self.tx.del(&self.db, key, value))?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, DatabaseError> {
        let r = internal_err!(self.tx.get(&self.db, key))?;
        Ok(r)
    }

    fn cursor(&mut self) -> Result<Box<dyn StateStoreCursor>, DatabaseError> {
        let cursor = internal_err!(self.tx.open_cursor(&self.db))?;
        Ok(Box::new(LmdbStateStoreCursor { cursor }))
    }

    fn commit(&mut self) -> Result<(), DatabaseError> {
        internal_err!(self.renew_tx())
    }
}

pub struct LmdbStateStoreCursor {
    cursor: Cursor,
}

impl StateStoreCursor for LmdbStateStoreCursor {
    fn seek(&mut self, key: &[u8]) -> Result<bool, DatabaseError> {
        internal_err!(self.cursor.seek(key))
    }

    fn next(&mut self) -> Result<bool, DatabaseError> {
        internal_err!(self.cursor.next())
    }

    fn prev(&mut self) -> Result<bool, DatabaseError> {
        internal_err!(self.cursor.prev())
    }

    fn read(&mut self) -> Result<Option<(&[u8], &[u8])>, DatabaseError> {
        internal_err!(self.cursor.read())
    }
}
