use crate::state::lmdb_sys::{
    Database, DatabaseOptions, EnvOptions, Environment, LmdbError, Transaction,
};
use std::sync::{Arc, RwLock};
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
    let mut tx = chk!(Transaction::begin(env.clone(), false));

    let mut db_opt = DatabaseOptions::default();
    db_opt.allow_duplicate_keys = true;
    let db = chk!(Database::open(env, &tx, "test".to_string(), Some(db_opt)));

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
    env_opt.no_locking = true;
    env_opt.no_thread_local_storage = true;

    let env = Arc::new(chk!(Environment::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        Some(env_opt)
    )));

    let mut tx = chk!(Transaction::begin(env.clone(), false));

    let mut db_opt = DatabaseOptions::default();
    db_opt.allow_duplicate_keys = false;
    let db = Arc::new(chk!(Database::open(
        env.clone(),
        &tx,
        "test".to_string(),
        Some(db_opt)
    )));
    let _created = tx.commit();

    let tx = Arc::new(RwLock::new(chk!(Transaction::begin(env.clone(), false))));

    let t1_db = db.clone();
    let t1_tx = tx.clone();

    let t1 = thread::spawn(move || -> Result<(), LmdbError> {
        for i in 1..=1000_u64 {
            let _r = t1_tx.write().unwrap().put(
                t1_db.as_ref(),
                &i.to_le_bytes(),
                &i.to_ne_bytes(),
                None,
            );
        }
        Ok(())
    });
    let _r1 = t1.join();

    let t2_db = db.clone();
    let t2_tx = tx.clone();

    let t2 = thread::spawn(move || -> Result<(), LmdbError> {
        for i in 1..=1000_u64 {
            let reader = t2_tx.read().unwrap();
            let _r = reader.get(t2_db.as_ref(), &i.to_le_bytes())?;
        }
        Ok(())
    });
    let _r2 = t2.join();
}
