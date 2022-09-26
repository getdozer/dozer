use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use crate::cache::utils;
use dozer_types::types::Record;
use lmdb::{Database, DatabaseFlags, Environment, RwTransaction, WriteFlags};

struct Cache<'a> {
    env: Environment,
    db: Database,
    txn: RefCell<Option<Arc<Mutex<RwTransaction<'a>>>>>,
}

impl Cache<'_> {
    fn new() -> Self {
        let (env, db) = utils::init_db();
        Self {
            env: env,
            db: db,
            txn: RefCell::new(None),
        }
    }

    pub fn begin_rw_txn<'env>(&'env self) {
        let txn = self.env.begin_rw_txn().unwrap();

        // self.txn.replace(Some(Arc::new(Mutex::new(txn))));
    }

    pub fn insert_record(&self, rec: Record) {
        let encoded: Vec<u8> = bincode::serialize(&rec).unwrap();

        self.txn
            .borrow_mut()
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .put::<String, Vec<u8>>(
                self.db,
                &"hello".to_string(),
                &encoded,
                WriteFlags::default(),
            )
            .unwrap();
    }
}
