use std::{sync::Arc, time::Duration};

use dozer_types::{
    crossbeam,
    log::debug,
    types::{Operation, Schema},
};
use lmdb::Transaction;

use crate::{
    cache::{index, LmdbCache},
    errors::CacheError,
};

pub struct BatchedCacheMsg {
    pub op: Operation,
    pub schema: Schema,
}

pub struct BatchedWriter {
    pub cache: Arc<LmdbCache>,
    pub receiver: crossbeam::channel::Receiver<BatchedCacheMsg>,
}
impl BatchedWriter {
    pub fn run(&self) -> Result<(), CacheError> {
        let mut disconnected = false;
        loop {
            if disconnected {
                break;
            }
            let mut txn = self.cache.init_txn();
            let mut idx = 0;
            loop {
                if idx > 500 {
                    break;
                }
                let msg = self.receiver.recv_timeout(Duration::from_millis(50));
                match msg {
                    Ok(msg) => {
                        let schema = msg.schema;
                        let op = msg.op;
                        match op {
                            Operation::Delete { old } => {
                                let key =
                                    index::get_primary_key(&schema.primary_index, &old.values);
                                self.cache._delete(&key, &mut txn)?;
                            }
                            Operation::Insert { new } => {
                                let mut new = new;
                                new.schema_id = schema.identifier.to_owned();

                                self.cache._insert(&mut txn, &new, &schema)?;
                            }
                            Operation::Update { old, new } => {
                                let key =
                                    index::get_primary_key(&schema.primary_index, &old.values);
                                let mut new = new;
                                new.schema_id = schema.identifier.clone();

                                self.cache._update(&key, &new, &schema, &mut txn)?;
                            }
                        }
                    }
                    Err(err) => match err {
                        crossbeam::channel::RecvTimeoutError::Timeout => {
                            // break the inner loop on timeout
                            debug!("Timeout in Batch Writer:");
                            break;
                        }
                        crossbeam::channel::RecvTimeoutError::Disconnected => {
                            disconnected = true;
                            break;
                        }
                    },
                }
                idx += 1;
            }
            debug!("Batch Writer: Commit");
            txn.commit().unwrap();
        }
        Ok(())
    }
}
