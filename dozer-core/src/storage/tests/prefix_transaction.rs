use std::fs;

use tempdir::TempDir;

use crate::storage::{
    lmdb_storage::{LmdbEnvironmentManager, SharedTransaction},
    prefix_transaction::PrefixTransaction,
};

#[test]
fn test_prefix_tx() {
    let tmp_dir = TempDir::new("example").unwrap();
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap();
    }
    fs::create_dir(tmp_dir.path()).unwrap();

    let mut env = LmdbEnvironmentManager::create(tmp_dir.path(), "test").unwrap();
    let db = env.open_database("test_db", false).unwrap();
    let tx = env.create_txn().unwrap();
    let mut tx = SharedTransaction::try_unwrap(tx).unwrap();

    const PREFIX0: u32 = 100;
    const PREFIX1: u32 = 101;
    const PREFIX2: u32 = 102;
    const PREFIX3: u32 = 103;

    let mut ptx0 = PrefixTransaction::new(&mut tx, PREFIX0);
    ptx0.put(db, "a0".as_bytes(), "a0".as_bytes()).unwrap();
    ptx0.put(db, "a1".as_bytes(), "a1".as_bytes()).unwrap();
    ptx0.put(db, "a2".as_bytes(), "a2".as_bytes()).unwrap();

    let mut ptx1 = PrefixTransaction::new(&mut tx, PREFIX1);
    ptx1.put(db, "b0".as_bytes(), "b0".as_bytes()).unwrap();
    ptx1.put(db, "b1".as_bytes(), "b1".as_bytes()).unwrap();
    ptx1.put(db, "b2".as_bytes(), "b2".as_bytes()).unwrap();

    let mut ptx2 = PrefixTransaction::new(&mut tx, PREFIX2);
    ptx2.put(db, "c0".as_bytes(), "c0".as_bytes()).unwrap();
    ptx2.put(db, "c1".as_bytes(), "c1".as_bytes()).unwrap();
    ptx2.put(db, "c2".as_bytes(), "c2".as_bytes()).unwrap();

    let ptx1 = PrefixTransaction::new(&mut tx, PREFIX1);
    let cur = ptx1.open_cursor(db).unwrap();
    let _r = cur.seek_gte("b0".as_bytes());
    let mut ctr = 0;
    loop {
        if let Some(_kv) = cur.read().unwrap() {
            ctr += 1;
        }
        if !cur.next().unwrap() {
            break;
        }
    }
    assert_eq!(ctr, 3);
    drop(cur);

    let ptx0 = PrefixTransaction::new(&mut tx, PREFIX0);
    assert_eq!(
        ptx0.get(db, "a0".as_bytes()).unwrap().unwrap(),
        "a0".as_bytes()
    );
    assert_eq!(ptx0.get(db, "b0".as_bytes()).unwrap(), None);

    let ptx1 = PrefixTransaction::new(&mut tx, PREFIX1);
    assert_eq!(
        ptx1.get(db, "b0".as_bytes()).unwrap().unwrap(),
        "b0".as_bytes()
    );
    assert_eq!(ptx1.get(db, "a0".as_bytes()).unwrap(), None);

    let ptx1_cur = ptx1.open_cursor(db).unwrap();

    assert!(ptx1_cur.seek_gte("b1".as_bytes()).unwrap());
    assert_eq!(ptx1_cur.read().unwrap().unwrap().0, "b1".as_bytes());

    assert!(ptx1_cur.first().unwrap());
    assert_eq!(ptx1_cur.read().unwrap().unwrap().0, "b0".as_bytes());

    assert!(ptx1_cur.last().unwrap());
    assert_eq!(ptx1_cur.read().unwrap().unwrap().0, "b2".as_bytes());

    drop(ptx1_cur);

    let ptx1_cur_1 = ptx1.open_cursor(db).unwrap();

    assert!(ptx1_cur_1.seek_gte("b1".as_bytes()).unwrap());
    assert_eq!(ptx1_cur_1.read().unwrap().unwrap().0, "b1".as_bytes());

    assert!(ptx1_cur_1.first().unwrap());
    assert_eq!(ptx1_cur_1.read().unwrap().unwrap().0, "b0".as_bytes());

    assert!(ptx1_cur_1.last().unwrap());
    assert_eq!(ptx1_cur_1.read().unwrap().unwrap().0, "b2".as_bytes());

    drop(ptx1_cur_1);

    let ptx2 = PrefixTransaction::new(&mut tx, PREFIX2);
    let ptx2_cur = ptx2.open_cursor(db).unwrap();

    assert!(ptx2_cur.seek_gte("c1".as_bytes()).unwrap());
    assert_eq!(ptx2_cur.read().unwrap().unwrap().0, "c1".as_bytes());

    assert!(ptx2_cur.first().unwrap());
    assert_eq!(ptx2_cur.read().unwrap().unwrap().0, "c0".as_bytes());

    assert!(ptx2_cur.last().unwrap());
    assert_eq!(ptx2_cur.read().unwrap().unwrap().0, "c2".as_bytes());

    drop(ptx2_cur);

    let ptx0 = PrefixTransaction::new(&mut tx, PREFIX0);
    let ptx0_cur = ptx0.open_cursor(db).unwrap();

    assert!(ptx0_cur.seek_gte("a1".as_bytes()).unwrap());
    assert_eq!(ptx0_cur.read().unwrap().unwrap().0, "a1".as_bytes());

    assert!(ptx0_cur.first().unwrap());
    assert_eq!(ptx0_cur.read().unwrap().unwrap().0, "a0".as_bytes());

    assert!(ptx0_cur.last().unwrap());
    assert_eq!(ptx0_cur.read().unwrap().unwrap().0, "a2".as_bytes());

    drop(ptx0_cur);

    let ptx3 = PrefixTransaction::new(&mut tx, PREFIX3);
    let ptx3_cur = ptx3.open_cursor(db).unwrap();

    assert!(!ptx3_cur.seek_gte("a1".as_bytes()).unwrap());
    assert_eq!(ptx3_cur.read().unwrap(), None);
    assert!(!ptx3_cur.first().unwrap());
    assert!(!ptx3_cur.last().unwrap());
}
