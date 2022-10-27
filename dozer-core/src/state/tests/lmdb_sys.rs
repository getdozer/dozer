use crate::state::lmdb_sys::{
    Database, DatabaseOptions, EnvOptions, Environment, LmdbError, Transaction,
};
use log::info;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{fs, thread};
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[test]
fn test_cursor_duplicate_keys() {
    let tmp_dir = chk!(TempDir::new("example"));
    if tmp_dir.path().exists() {
        chk!(fs::remove_dir_all(tmp_dir.path()));
    }
    chk!(fs::create_dir(tmp_dir.path()));

    let mut env_opt = EnvOptions::default();
    env_opt.no_sync = true;
    env_opt.max_dbs = Some(10);
    env_opt.map_size = Some(1024 * 1024 * 1024);
    env_opt.writable_mem_map = true;

    let env = Arc::new(chk!(Environment::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        Some(env_opt)
    )));
    let mut tx = chk!(Transaction::begin(&env, false));

    let mut db_opt = DatabaseOptions::default();
    db_opt.allow_duplicate_keys = true;
    let db = chk!(Database::open(&env, &tx, "test".to_string(), Some(db_opt)));

    for k in 1..3 {
        for i in 'a'..'s' {
            chk!(tx.put(
                &db,
                format!("key_{}", k).as_bytes(),
                format!("val_{}", i).as_bytes(),
                None,
            ));
        }
    }

    let cursor = chk!(tx.open_cursor(&db));

    let r = chk!(cursor.seek("key_100".as_bytes()));
    assert!(!r);

    let r = chk!(cursor.seek("key_1".as_bytes()));
    assert!(r);

    for i in 'a'..='z' {
        let r = chk!(cursor.read()).unwrap();
        if r.0 != "key_1".as_bytes() {
            break;
        }

        assert_eq!(r.0, "key_1".as_bytes());
        assert_eq!(r.1, format!("val_{}", i).as_bytes());
        let _r = chk!(cursor.next());
    }

    for i in 'a'..='z' {
        let r = chk!(cursor.read()).unwrap();
        assert_eq!(r.0, "key_2".as_bytes());
        assert_eq!(r.1, format!("val_{}", i).as_bytes());
        let r = chk!(cursor.next());

        if !r {
            break;
        }
    }
}

#[test]
fn test_concurrent_tx() {
    log4rs::init_file("../log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let tmp_dir = chk!(TempDir::new("example"));
    if tmp_dir.path().exists() {
        chk!(fs::remove_dir_all(tmp_dir.path()));
    }
    chk!(fs::create_dir(tmp_dir.path()));

    let mut env_opt = EnvOptions::default();
    env_opt.no_sync = true;
    env_opt.max_dbs = Some(10);
    env_opt.max_readers = Some(10);
    env_opt.map_size = Some(1024 * 1024 * 1024);
    env_opt.writable_mem_map = false;
    env_opt.no_sync = true;
    env_opt.no_locking = true;
    env_opt.no_thread_local_storage = true;

    let mut env = chk!(Environment::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        Some(env_opt)
    ));

    let mut tx = chk!(env.tx_begin(false));
    let mut db_opt = DatabaseOptions::default();
    db_opt.allow_duplicate_keys = false;
    let db = chk!(Database::open(&env, &tx, "test".to_string(), Some(db_opt)));
    chk!(tx.commit());

    let mut env_t1 = env.clone();
    let mut db_t1 = db.clone();
    let t1 = thread::spawn(move || -> Result<(), LmdbError> {
        for i in 1..=1_000_000_u64 {
            let mut tx = chk!(env_t1.tx_begin(false));
            tx.put(&db_t1, &i.to_le_bytes(), &i.to_le_bytes(), None)?;
            chk!(tx.commit());
            if i % 10000 == 0 {
                info!("Writer: {}", i)
            }
        }
        Ok(())
    });

    thread::sleep(Duration::from_millis(200));

    let mut env_t2 = env.clone();
    let mut db_t2 = db.clone();
    let t2 = thread::spawn(move || -> Result<(), LmdbError> {
        for i in 1..=1_000_000_u64 {
            let mut tx = chk!(env_t2.tx_begin(true));
            let v = tx.get(&db_t2, &i.to_le_bytes())?;
            if v.is_none() {
                info!("{}, v is none", i);
            }
            if i % 1000 == 0 {
                info!("Reader 1: {}", i)
            }
            thread::sleep(Duration::from_micros(8));
        }
        Ok(())
    });

    let mut env_t3 = env.clone();
    let mut db_t3 = db.clone();
    let t3 = thread::spawn(move || -> Result<(), LmdbError> {
        for i in 1..=1_000_000_u64 {
            let mut tx = chk!(env_t3.tx_begin(true));
            let v = tx.get(&db_t3, &i.to_le_bytes())?;
            if v.is_none() {
                info!("{}, v is none", i);
            }
            if i % 1000 == 0 {
                info!("Reader 2: {}", i)
            }
            thread::sleep(Duration::from_micros(8));
        }
        Ok(())
    });

    let r1 = t1.join();
    assert!(r1.is_ok());
    let r2 = t2.join();
    assert!(r2.is_ok());
    let r3 = t3.join();
    assert!(r3.is_ok());
}
