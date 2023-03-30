use dozer_types::log::info;
use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use std::{fs, thread};
use tempdir::TempDir;

use crate::errors::StorageError;

fn create_env() -> (Environment, Database) {
    let tmp_dir = TempDir::new("concurrent").unwrap();
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap();
    }
    fs::create_dir(tmp_dir.path()).unwrap();

    let mut builder = Environment::new();
    builder.set_flags(EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_TLS);
    builder.set_max_dbs(10);
    builder.set_max_readers(10);
    builder.set_map_size(1024 * 1024 * 1024);

    let env = builder.open(tmp_dir.path()).unwrap();
    let db = env
        .create_db(Some("test"), DatabaseFlags::default())
        .unwrap();

    (env, db)
}

#[test]
fn test_concurrent_tx() {
    //  log4rs::init_file("./log4rs.yaml", Default::default())
    //      .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let e1 = create_env();
    let e2 = create_env();

    let t1 = thread::spawn(move || -> Result<(), StorageError> {
        for i in 1..=1_000_u64 {
            let mut tx = e1.0.begin_rw_txn().unwrap();
            tx.put(
                e1.1,
                &i.to_le_bytes(),
                &i.to_le_bytes(),
                WriteFlags::default(),
            )?;
            tx.commit().unwrap();
            if i % 10000 == 0 {
                info!("Writer 1: {}", i)
            }
        }
        Ok(())
    });

    let t2 = thread::spawn(move || -> Result<(), StorageError> {
        for i in 1..=1_000_u64 {
            let mut tx = e2.0.begin_rw_txn().unwrap();
            tx.put(
                e2.1,
                &i.to_le_bytes(),
                &i.to_le_bytes(),
                WriteFlags::default(),
            )?;
            tx.commit().unwrap();
            if i % 10000 == 0 {
                info!("Writer 2: {}", i)
            }
        }
        Ok(())
    });

    let r1 = t1.join();
    assert!(r1.is_ok());
    let r2 = t2.join();
    assert!(r2.is_ok());
}
