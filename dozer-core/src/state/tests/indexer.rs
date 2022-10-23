use crate::state::indexer::{SecondaryIndexKey, StateStoreIndexer};
use crate::state::lmdb::LmdbStateStoreManager;
use dozer_types::core::state::{StateStoreOptions, StateStoresManager};
use std::fs;
use tempdir::TempDir;

#[test]
fn test_cursor_duplicate_keys() {
    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let ssm = LmdbStateStoreManager::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        1024 * 1024 * 1024,
        10000,
    );

    let mut ss = ssm
        .init_state_store(
            "test".to_string(),
            StateStoreOptions {
                allow_duplicate_keys: true,
            },
        )
        .unwrap_or_else(|_e| panic!("Unable to create state store"));

    let mut indexer = StateStoreIndexer::new(10_u16);

    let _r = indexer
        .put(
            ss.as_mut(),
            Some("pk1".as_bytes()),
            "val1".as_bytes(),
            vec![
                SecondaryIndexKey::new(100, "A".as_bytes()),
                SecondaryIndexKey::new(200, "B0".as_bytes()),
            ],
        )
        .unwrap_or_else(|_e| panic!("Error during indexer put"));

    let _r = indexer
        .put(
            ss.as_mut(),
            Some("pk2".as_bytes()),
            "val2".as_bytes(),
            vec![
                SecondaryIndexKey::new(100, "A".as_bytes()),
                SecondaryIndexKey::new(200, "B1".as_bytes()),
            ],
        )
        .unwrap_or_else(|_e| panic!("Error during indexer put"));

    let _r = indexer
        .put(
            ss.as_mut(),
            Some("pk3".as_bytes()),
            "val3".as_bytes(),
            vec![
                SecondaryIndexKey::new(100, "A".as_bytes()),
                SecondaryIndexKey::new(200, "B2".as_bytes()),
            ],
        )
        .unwrap_or_else(|_e| panic!("Error during indexer put"));

    let v = indexer
        .get_multi(ss.as_mut(), 100, "A".as_bytes())
        .unwrap_or_else(|_e| panic!("Error during indexer get"));
    assert_eq!(
        v,
        vec!["val1".as_bytes(), "val2".as_bytes(), "val3".as_bytes()]
    );

    let v = indexer
        .get_multi(ss.as_mut(), 200, "B1".as_bytes())
        .unwrap_or_else(|_e| panic!("Error during indexer get"));
    assert_eq!(v, vec!["val2".as_bytes()]);

    indexer
        .del(ss.as_mut(), "pk3".as_bytes())
        .unwrap_or_else(|_e| panic!("Error during indexer del"));

    let v = indexer
        .get_multi(ss.as_mut(), 100, "A".as_bytes())
        .unwrap_or_else(|_e| panic!("Error during indexer get"));
    assert_eq!(v, vec!["val1".as_bytes(), "val2".as_bytes()]);

    let _r = indexer
        .put(
            ss.as_mut(),
            Some("pk3".as_bytes()),
            "val3".as_bytes(),
            vec![
                SecondaryIndexKey::new(100, "A1".as_bytes()),
                SecondaryIndexKey::new(200, "B2".as_bytes()),
            ],
        )
        .unwrap_or_else(|_e| panic!("Error during indexer put"));

    let v = indexer
        .get_multi(ss.as_mut(), 100, "A1".as_bytes())
        .unwrap_or_else(|_e| panic!("Error during indexer get"));
    assert_eq!(v, vec!["val3".as_bytes()]);

    let v = indexer
        .get_multi(ss.as_mut(), 100, "C".as_bytes())
        .unwrap_or_else(|_e| panic!("Error during indexer get"));
    assert_eq!(v, Vec::<&[u8]>::new());
}
