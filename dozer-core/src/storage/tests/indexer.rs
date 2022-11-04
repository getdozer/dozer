// use crate::storage::indexer::{SecondaryIndexKey, StateStoreIndexer};
// use std::fs;
// use tempdir::TempDir;
//
// macro_rules! chk {
//     ($stmt:expr) => {
//         $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
//     };
// }
//
// #[test]
// fn test_cursor_duplicate_keys() {
//     let tmp_dir = chk!(TempDir::new("example"));
//     if tmp_dir.path().exists() {
//         chk!(fs::remove_dir_all(tmp_dir.path()));
//     }
//     chk!(fs::create_dir(tmp_dir.path()));
//
//     let ssm = LmdbStateStoreManager::new(
//         tmp_dir.path().to_str().unwrap().to_string(),
//         1024 * 1024 * 1024,
//         10000,
//     );
//
//     let mut ss = chk!(ssm.init_state_store(
//         "test".to_string(),
//         StateStoreOptions {
//             allow_duplicate_keys: true,
//         },
//     ));
//
//     let mut indexer = StateStoreIndexer::new(10_u16);
//
//     let _r = chk!(indexer.put(
//         ss.as_mut(),
//         Some("pk1".as_bytes()),
//         "val1".as_bytes(),
//         vec![
//             SecondaryIndexKey::new(100, "A".as_bytes()),
//             SecondaryIndexKey::new(200, "B0".as_bytes()),
//         ],
//     ));
//
//     let _r = chk!(indexer.put(
//         ss.as_mut(),
//         Some("pk2".as_bytes()),
//         "val2".as_bytes(),
//         vec![
//             SecondaryIndexKey::new(100, "A".as_bytes()),
//             SecondaryIndexKey::new(200, "B1".as_bytes()),
//         ],
//     ));
//
//     let _r = chk!(indexer.put(
//         ss.as_mut(),
//         Some("pk3".as_bytes()),
//         "val3".as_bytes(),
//         vec![
//             SecondaryIndexKey::new(100, "A".as_bytes()),
//             SecondaryIndexKey::new(200, "B2".as_bytes()),
//         ],
//     ));
//
//     let v = chk!(indexer.get_multi(ss.as_mut(), 100, "A".as_bytes()));
//     assert_eq!(
//         v,
//         vec!["val1".as_bytes(), "val2".as_bytes(), "val3".as_bytes()]
//     );
//
//     let v = chk!(indexer.get_multi(ss.as_mut(), 200, "B1".as_bytes()));
//     assert_eq!(v, vec!["val2".as_bytes()]);
//
//     chk!(indexer.del(ss.as_mut(), "pk3".as_bytes()));
//
//     let v = chk!(indexer.get_multi(ss.as_mut(), 100, "A".as_bytes()));
//     assert_eq!(v, vec!["val1".as_bytes(), "val2".as_bytes()]);
//
//     let _r = chk!(indexer.put(
//         ss.as_mut(),
//         Some("pk3".as_bytes()),
//         "val3".as_bytes(),
//         vec![
//             SecondaryIndexKey::new(100, "A1".as_bytes()),
//             SecondaryIndexKey::new(200, "B2".as_bytes()),
//         ],
//     ));
//
//     let v = chk!(indexer.get_multi(ss.as_mut(), 100, "A1".as_bytes()));
//     assert_eq!(v, vec!["val3".as_bytes()]);
//
//     let v = chk!(indexer.get_multi(ss.as_mut(), 100, "C".as_bytes()));
//     assert_eq!(v, Vec::<&[u8]>::new());
// }
