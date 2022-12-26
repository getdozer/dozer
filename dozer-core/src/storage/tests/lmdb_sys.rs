use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use log::info;
use std::{fs, thread};
use tempdir::TempDir;

use crate::storage::{common::Seek, errors::StorageError};

#[test]
fn test_cursor_duplicate_keys() {
    let tmp_dir = TempDir::new("example").unwrap();
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap();
    }
    fs::create_dir(tmp_dir.path()).unwrap();

    let mut builder = Environment::new();
    builder.set_flags(EnvironmentFlags::NO_SYNC | EnvironmentFlags::WRITE_MAP);
    builder.set_max_dbs(10);
    builder.set_map_size(1024 * 1024 * 1024);

    let env = builder.open(tmp_dir.path()).unwrap();
    let db = env
        .create_db(Some("test"), DatabaseFlags::DUP_SORT)
        .unwrap();

    let mut tx = env.begin_rw_txn().unwrap();

    for k in 1..3 {
        for i in 'a'..'s' {
            tx.put(
                db,
                &format!("key_{}", k).as_bytes(),
                &format!("val_{}", i).as_bytes(),
                WriteFlags::default(),
            )
            .unwrap();
        }
    }

    let cursor = tx.open_ro_cursor(db).unwrap();

    let r = cursor.seek("key_100".as_bytes()).unwrap();
    assert!(!r);

    let r = cursor.seek("key_1".as_bytes()).unwrap();
    assert!(r);

    for i in 'a'..='z' {
        let r = cursor.read().unwrap().unwrap();
        if r.0 != "key_1".as_bytes() {
            break;
        }

        assert_eq!(r.0, "key_1".as_bytes());
        assert_eq!(r.1, format!("val_{}", i).as_bytes());
        let _r = cursor.next().unwrap();
    }

    for i in 'a'..='z' {
        let r = cursor.read().unwrap().unwrap();
        assert_eq!(r.0, "key_2".as_bytes());
        assert_eq!(r.1, format!("val_{}", i).as_bytes());
        let r = cursor.next().unwrap();

        if !r {
            break;
        }
    }
}

fn create_env() -> (Environment, Database) {
    let tmp_dir = TempDir::new("concurrent").unwrap();
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap();
    }
    fs::create_dir(tmp_dir.path()).unwrap();

    let mut builder = Environment::new();
    builder.set_flags(
        EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_LOCK | EnvironmentFlags::NO_TLS,
    );
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
