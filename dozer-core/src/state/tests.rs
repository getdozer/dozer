use crate::state::lmdb_sys::{Database, DatabaseOptions, EnvOptions, Environment, Transaction};
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

#[test]
fn test_cursor_duplicate_keys() {
    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let mut env_opt = EnvOptions::default();
    env_opt.no_sync = true;
    env_opt.max_dbs = Some(10);
    env_opt.map_size = Some(1024 * 1024 * 1024);
    env_opt.writable_mem_map = true;

    let env = Arc::new(
        Environment::new(tmp_dir.path().to_str().unwrap().to_string(), Some(env_opt))
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
    );
    let tx = Transaction::begin(env.clone()).unwrap_or_else(|e| panic!("{}", e.to_string()));

    let mut db_opt = DatabaseOptions::default();
    db_opt.allow_duplicate_keys = true;
    let db = Database::open(env.clone(), &tx, "test".to_string(), Some(db_opt))
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    for k in 1..3 {
        for i in 'a'..'s' {
            db.put(
                &tx,
                format!("key_{}", k).as_bytes(),
                format!("val_{}", i).as_bytes(),
                None,
            )
            .unwrap_or_else(|e| panic!("{}", e.to_string()));
        }
    }

    let cursor = db
        .open_cursor(&tx)
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let r = cursor
        .seek("key_100".as_bytes())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));
    assert!(!r);

    let r = cursor
        .seek("key_1".as_bytes())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));
    assert!(r);

    for i in 'a'..'z' {
        let r = cursor
            .read()
            .unwrap_or_else(|e| panic!("{}", e.to_string()))
            .unwrap();

        if r.0 != "key_1".as_bytes() {
            break;
        }

        assert_eq!(r.0, "key_1".as_bytes());
        assert_eq!(r.1, format!("val_{}", i).as_bytes());

        let r = cursor
            .next()
            .unwrap_or_else(|e| panic!("{}", e.to_string()));
    }

    for i in 'a'..'z' {
        let r = cursor
            .read()
            .unwrap_or_else(|e| panic!("{}", e.to_string()))
            .unwrap();
        assert_eq!(r.0, "key_2".as_bytes());
        assert_eq!(r.1, format!("val_{}", i).as_bytes());

        let r = cursor
            .next()
            .unwrap_or_else(|e| panic!("{}", e.to_string()));

        if !r {
            break;
        }
    }
}
