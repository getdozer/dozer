use crate::storage::common::{RenewableRwTransaction, RwTransaction};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use crate::storage::prefix_transaction::PrefixTransaction;
use crate::storage::transactions::SharedTransaction;
use dozer_types::parking_lot::RwLock;
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[test]
fn test_prefix_tx() {
    let tmp_dir = chk!(TempDir::new("example"));
    if tmp_dir.path().exists() {
        chk!(fs::remove_dir_all(tmp_dir.path()));
    }
    chk!(fs::create_dir(tmp_dir.path()));

    let mut env = chk!(LmdbEnvironmentManager::create(tmp_dir.path(), "test"));
    let db = chk!(env.open_database("test_db", false));
    let tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
        Arc::new(RwLock::new(chk!(env.create_txn())));

    let tx0 = tx.clone();
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    let tx3 = tx.clone();

    let mut shared0 = SharedTransaction::new(&tx0);
    let mut shared1 = SharedTransaction::new(&tx1);
    let mut shared2 = SharedTransaction::new(&tx2);
    let mut shared3 = SharedTransaction::new(&tx3);

    let mut ptx0 = PrefixTransaction::new(&mut shared0, 100);
    let mut ptx1 = PrefixTransaction::new(&mut shared1, 101);
    let mut ptx2 = PrefixTransaction::new(&mut shared2, 102);
    let ptx3 = PrefixTransaction::new(&mut shared3, 103);

    chk!(ptx0.put(&db, "a0".as_bytes(), "a0".as_bytes()));
    chk!(ptx0.put(&db, "a1".as_bytes(), "a1".as_bytes()));
    chk!(ptx0.put(&db, "a2".as_bytes(), "a2".as_bytes()));

    chk!(ptx1.put(&db, "b0".as_bytes(), "b0".as_bytes()));
    chk!(ptx1.put(&db, "b1".as_bytes(), "b1".as_bytes()));
    chk!(ptx1.put(&db, "b2".as_bytes(), "b2".as_bytes()));

    chk!(ptx2.put(&db, "c0".as_bytes(), "c0".as_bytes()));
    chk!(ptx2.put(&db, "c1".as_bytes(), "c1".as_bytes()));
    chk!(ptx2.put(&db, "c2".as_bytes(), "c2".as_bytes()));

    assert_eq!(
        chk!(ptx0.get(&db, "a0".as_bytes())).unwrap(),
        "a0".as_bytes()
    );
    assert_eq!(chk!(ptx0.get(&db, "b0".as_bytes())), None);

    assert_eq!(
        chk!(ptx1.get(&db, "b0".as_bytes())).unwrap(),
        "b0".as_bytes()
    );
    assert_eq!(chk!(ptx1.get(&db, "a0".as_bytes())), None);

    let ptx1_cur = chk!(ptx1.open_cursor(&db));

    assert!(chk!(ptx1_cur.seek_gte("b1".as_bytes())));
    assert_eq!(chk!(ptx1_cur.read()).unwrap().0, "b1".as_bytes());

    assert!(chk!(ptx1_cur.first()));
    assert_eq!(chk!(ptx1_cur.read()).unwrap().0, "b0".as_bytes());

    assert!(chk!(ptx1_cur.last()));
    assert_eq!(chk!(ptx1_cur.read()).unwrap().0, "b2".as_bytes());

    let ptx1_cur_1 = chk!(ptx1.open_cursor(&db));

    assert!(chk!(ptx1_cur_1.seek_gte("b1".as_bytes())));
    assert_eq!(chk!(ptx1_cur_1.read()).unwrap().0, "b1".as_bytes());

    assert!(chk!(ptx1_cur_1.first()));
    assert_eq!(chk!(ptx1_cur_1.read()).unwrap().0, "b0".as_bytes());

    assert!(chk!(ptx1_cur_1.last()));
    assert_eq!(chk!(ptx1_cur_1.read()).unwrap().0, "b2".as_bytes());

    let ptx2_cur = chk!(ptx2.open_cursor(&db));

    assert!(chk!(ptx2_cur.seek_gte("c1".as_bytes())));
    assert_eq!(chk!(ptx2_cur.read()).unwrap().0, "c1".as_bytes());

    assert!(chk!(ptx2_cur.first()));
    assert_eq!(chk!(ptx2_cur.read()).unwrap().0, "c0".as_bytes());

    assert!(chk!(ptx2_cur.last()));
    assert_eq!(chk!(ptx2_cur.read()).unwrap().0, "c2".as_bytes());

    let ptx0_cur = chk!(ptx0.open_cursor(&db));

    assert!(chk!(ptx0_cur.seek_gte("a1".as_bytes())));
    assert_eq!(chk!(ptx0_cur.read()).unwrap().0, "a1".as_bytes());

    assert!(chk!(ptx0_cur.first()));
    assert_eq!(chk!(ptx0_cur.read()).unwrap().0, "a0".as_bytes());

    assert!(chk!(ptx0_cur.last()));
    assert_eq!(chk!(ptx0_cur.read()).unwrap().0, "a2".as_bytes());

    let ptx3_cur = chk!(ptx3.open_cursor(&db));

    assert!(!chk!(ptx3_cur.seek_gte("a1".as_bytes())));
    assert_eq!(chk!(ptx3_cur.read()), None);
    assert!(!chk!(ptx3_cur.first()));
    assert!(!chk!(ptx3_cur.last()));

    chk!(ptx3_cur.put("d0".as_bytes(), "d1".as_bytes()));
    chk!(ptx3_cur.put("d1".as_bytes(), "d2".as_bytes()));
    chk!(ptx3_cur.put("d2".as_bytes(), "d2".as_bytes()));

    assert!(chk!(ptx3_cur.seek_gte("d1".as_bytes())));
    assert_eq!(chk!(ptx3_cur.read()).unwrap().0, "d1".as_bytes());

    assert!(chk!(ptx3_cur.first()));
    assert_eq!(chk!(ptx3_cur.read()).unwrap().0, "d0".as_bytes());

    assert!(chk!(ptx3_cur.last()));
    assert_eq!(chk!(ptx3_cur.read()).unwrap().0, "d2".as_bytes());
}
