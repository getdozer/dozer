use crate::cache::utils;
use dozer_shared::types::Record;
use lmdb::{Database, DatabaseFlags, Environment, RwTransaction, WriteFlags};

struct Cache<'a> {
    env: &'a Environment,
    db: &'a Database,
    txn: Option<RwTransaction<'a>>,
}

impl Cache<'_> {
    fn new() -> Self {
        let (env, db) = utils::init_db();
        Self {
            env: &env,
            db: &db,
            txn: None,
        }
    }

    pub fn begin_rw_txn<'env>(&'env self) {
        let txn = self.env.begin_rw_txn().unwrap();

        self.txn = Some(txn);
    }

    pub fn insert_record(&self, rec: Record) {
        let txn = self.txn.unwrap();
        let encoded: Vec<u8> = bincode::serialize(&rec).unwrap();

        txn.put::<String, Vec<u8>>(
            *self.db,
            &"hello".to_string(),
            &encoded,
            WriteFlags::default(),
        )
        .unwrap();
    }
}
