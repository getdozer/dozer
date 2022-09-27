use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use crate::cache::utils;
use dozer_types::types::Record;
use lmdb::{Database, Environment, RwTransaction, WriteFlags};

struct Cache<'a> {
    env: Environment,
    db: Database,
    txn: RefCell<Option<Mutex<RwTransaction<'a>>>>,
}

impl<'a> Cache<'a> {
    fn new() -> Self {
        let (env, db) = utils::init_db();
        Self {
            env: env,
            db: db,
            txn: RefCell::new(None),
        }
    }

    pub fn begin(&'a self) {
        let txn = self.env.begin_rw_txn().unwrap();

        self.txn.replace(Some(Mutex::new(txn)));
    }

    pub fn insert(&self, rec: Record) {
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

    pub fn delete(&self, key: String) {}

    pub fn get(&self, key: String) -> Record {
        todo!()
    }

    pub fn query(&self, key: String) -> Vec<Record> {
        todo!()
    }
}
