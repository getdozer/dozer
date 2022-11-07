use crate::storage::indexed_db::{IndexedDatabase, SecondaryIndexKey};
use crate::storage::lmdb_sys::{DatabaseOptions, EnvOptions, Environment};
use std::fs;
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
    env_opt.max_dbs = Some(5);
    env_opt.map_size = Some(1024 * 1024 * 10);
    env_opt.max_readers = Some(5);
    env_opt.writable_mem_map = true;
    env_opt.no_subdir = false;

    let mut env = chk!(Environment::new(
        tmp_dir.into_path().to_str().unwrap().to_string(),
        env_opt
    ));
    let mut tx = chk!(env.tx_begin(false));

    let mut db_opts = DatabaseOptions::default();
    db_opts.allow_duplicate_keys = true;

    let db = chk!(tx.open_database("test".to_string(), db_opts));
    let mut indexer = IndexedDatabase::new(db);

    let _r = chk!(indexer.put(
        &mut tx,
        Some("pk1".as_bytes()),
        "val1".as_bytes(),
        vec![
            SecondaryIndexKey::new(100, "A".as_bytes()),
            SecondaryIndexKey::new(200, "B0".as_bytes()),
        ],
    ));

    let _r = chk!(indexer.put(
        &mut tx,
        Some("pk2".as_bytes()),
        "val2".as_bytes(),
        vec![
            SecondaryIndexKey::new(100, "A".as_bytes()),
            SecondaryIndexKey::new(200, "B1".as_bytes()),
        ],
    ));

    let _r = chk!(indexer.put(
        &mut tx,
        Some("pk3".as_bytes()),
        "val3".as_bytes(),
        vec![
            SecondaryIndexKey::new(100, "A".as_bytes()),
            SecondaryIndexKey::new(200, "B2".as_bytes()),
        ],
    ));

    let v = chk!(indexer.get_multi(&mut tx, 100, "A".as_bytes()));
    assert_eq!(
        v,
        vec!["val1".as_bytes(), "val2".as_bytes(), "val3".as_bytes()]
    );

    let v = chk!(indexer.get_multi(&mut tx, 200, "B1".as_bytes()));
    assert_eq!(v, vec!["val2".as_bytes()]);

    chk!(indexer.del(&mut tx, "pk3".as_bytes()));

    let v = chk!(indexer.get_multi(&mut tx, 100, "A".as_bytes()));
    assert_eq!(v, vec!["val1".as_bytes(), "val2".as_bytes()]);

    let _r = chk!(indexer.put(
        &mut tx,
        Some("pk3".as_bytes()),
        "val3".as_bytes(),
        vec![
            SecondaryIndexKey::new(100, "A1".as_bytes()),
            SecondaryIndexKey::new(200, "B2".as_bytes()),
        ],
    ));

    let v = chk!(indexer.get_multi(&mut tx, 100, "A1".as_bytes()));
    assert_eq!(v, vec!["val3".as_bytes()]);

    let v = chk!(indexer.get_multi(&mut tx, 100, "C".as_bytes()));
    assert_eq!(v, Vec::<&[u8]>::new());
}
