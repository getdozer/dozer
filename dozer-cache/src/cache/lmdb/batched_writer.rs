use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dozer_types::{
    crossbeam,
    log::info,
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
    pub record_cutoff: u32,
    pub timeout: u16,
}
impl BatchedWriter {
    pub fn run(&self) -> Result<(), CacheError> {
        let mut disconnected = false;
        let before = Instant::now();
        let mut commits = 0;
        let mut total_idx = 0;
        loop {
            if disconnected {
                break;
            }
            let mut txn = self.cache.init_txn();
            let mut idx = 0;

            loop {
                if idx > self.record_cutoff {
                    info!("record_cutoff in Batch Writer ");
                    break;
                }
                let msg = self.receiver.recv_timeout(Duration::from_millis(300));
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
                            info!("Timeout in Batch Writer: {}", idx);
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
            total_idx += idx;

            if idx > self.record_cutoff {
                info!(
                    "Batch Writer: Commit : {} : {total_idx}: elapsed: {:.2?}",
                    commits,
                    before.elapsed()
                );

                txn.commit().unwrap();

                commits += 1;
            }
        }
        Ok(())
    }
}
