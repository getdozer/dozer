use dozer_storage::{assert_database_equal, lmdb::Transaction};
use dozer_types::{borrow::IntoOwned, types::Record};

use crate::cache::{
    lmdb::{
        cache::main_environment::{
            operation_log::{MetadataKey, OperationLog, INITIAL_RECORD_VERSION},
            Operation,
        },
        utils::create_env,
    },
    CacheRecord,
};

pub fn assert_operation_log_equal<T1: Transaction, T2: Transaction>(
    log1: &OperationLog,
    txn1: &T1,
    log2: &OperationLog,
    txn2: &T2,
) {
    assert_database_equal(
        txn1,
        log1.primary_key_metadata.database(),
        txn2,
        log2.primary_key_metadata.database(),
    );
    assert_database_equal(
        txn1,
        log1.hash_metadata.database(),
        txn2,
        log2.hash_metadata.database(),
    );
    assert_database_equal(
        txn1,
        log1.present_operation_ids.database(),
        txn2,
        log2.present_operation_ids.database(),
    );
    assert_database_equal(
        txn1,
        log1.next_operation_id.database(),
        txn2,
        log2.next_operation_id.database(),
    );
    assert_database_equal(
        txn1,
        log1.operation_id_to_operation.database(),
        txn2,
        log2.operation_id_to_operation.database(),
    );
}

#[test]
fn test_operation_log_append_only() {
    let mut env = create_env(&Default::default()).unwrap().0;
    let log = OperationLog::create(&mut env, Default::default()).unwrap();
    let txn = env.txn_mut().unwrap();
    let append_only = true;

    let records = vec![Record::new(vec![]); 10];
    for (index, record) in records.iter().enumerate() {
        let record_meta = log.insert_new(txn, None, record).unwrap();
        assert_eq!(record_meta.id, index as u64);
        assert_eq!(record_meta.version, INITIAL_RECORD_VERSION);
        assert_eq!(
            log.count_present_records(txn, append_only).unwrap(),
            index + 1
        );
        assert_eq!(log.next_operation_id(txn).unwrap(), index as u64 + 1);
        assert_eq!(
            log.present_operation_ids(txn, append_only)
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            (0..=index as u64).collect::<Vec<_>>()
        );
        assert!(log
            .contains_operation_id(txn, append_only, index as _)
            .unwrap());
        assert_eq!(
            log.get_record_by_operation_id_unchecked(txn, index as _)
                .unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(
            log.get_operation(txn, index as _).unwrap().unwrap(),
            Operation::Insert {
                record_meta,
                record: record.clone(),
            }
        );
    }
}

#[test]
fn test_operation_log_with_primary_key() {
    let mut env = create_env(&Default::default()).unwrap().0;
    let log = OperationLog::create(&mut env, Default::default()).unwrap();
    let txn = env.txn_mut().unwrap();
    let append_only = false;

    // Insert a record.
    let record = Record::new(vec![]);
    let primary_key = b"primary_key";
    let key = MetadataKey::PrimaryKey(primary_key);
    let mut record_meta = log.insert_new(txn, Some(key), &record).unwrap();
    assert_eq!(record_meta.id, 0);
    assert_eq!(record_meta.version, INITIAL_RECORD_VERSION);
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
    assert_eq!(
        log.get_record(txn, primary_key).unwrap().unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(log.next_operation_id(txn).unwrap(), 1);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0]
    );
    assert!(log.contains_operation_id(txn, append_only, 0).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 0).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 0).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone(),
        }
    );

    // Update the record.
    record_meta = log.update(txn, key, &record, record_meta, 0).unwrap();
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
    assert_eq!(
        log.get_record(txn, primary_key).unwrap().unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(log.next_operation_id(txn).unwrap(), 3);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![2]
    );
    assert!(log.contains_operation_id(txn, append_only, 2).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 2).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 1).unwrap().unwrap(),
        Operation::Delete { operation_id: 0 }
    );
    assert_eq!(
        log.get_operation(txn, 2).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone()
        }
    );

    // Delete the record.
    log.delete(txn, key, record_meta, 2).unwrap();
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 0);
    assert_eq!(log.get_record(txn, primary_key).unwrap(), None);
    assert_eq!(log.next_operation_id(txn).unwrap(), 4);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        Vec::<u64>::new(),
    );
    assert!(!log.contains_operation_id(txn, append_only, 2).unwrap());
    assert_eq!(
        log.get_operation(txn, 3).unwrap().unwrap(),
        Operation::Delete { operation_id: 2 }
    );

    // Insert with that primary key again.
    record_meta = log.insert_deleted(txn, key, &record, record_meta).unwrap();
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
    assert_eq!(
        log.get_record(txn, primary_key).unwrap().unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(log.next_operation_id(txn).unwrap(), 5);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![4]
    );
    assert!(log.contains_operation_id(txn, append_only, 4).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 4).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 4).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone(),
        }
    );
}

#[test]
fn test_operation_log_without_primary_key() {
    let mut env = create_env(&Default::default()).unwrap().0;
    let log = OperationLog::create(&mut env, Default::default()).unwrap();
    let txn = env.txn_mut().unwrap();
    let append_only = false;

    // Insert a record.
    let record = Record::new(vec![]);
    let key = MetadataKey::Hash(&record, 0);
    let mut record_meta = log.insert_new(txn, Some(key), &record).unwrap();
    assert_eq!(record_meta.id, 0);
    assert_eq!(record_meta.version, INITIAL_RECORD_VERSION);
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
    assert_eq!(log.next_operation_id(txn).unwrap(), 1);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0]
    );
    assert!(log.contains_operation_id(txn, append_only, 0).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 0).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 0).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone(),
        }
    );

    // Insert the same record again.
    record_meta = log.insert_new(txn, Some(key), &record).unwrap();
    assert_eq!(record_meta.id, 1);
    assert_eq!(record_meta.version, INITIAL_RECORD_VERSION);
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 2);
    assert_eq!(log.next_operation_id(txn).unwrap(), 2);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0, 1]
    );
    assert!(log.contains_operation_id(txn, append_only, 1).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 1).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 1).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone(),
        }
    );

    // Update the record.
    record_meta = log.update(txn, key, &record, record_meta, 1).unwrap();
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 2);
    assert_eq!(log.next_operation_id(txn).unwrap(), 4);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0, 3]
    );
    assert!(log.contains_operation_id(txn, append_only, 3).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 3).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 2).unwrap().unwrap(),
        Operation::Delete { operation_id: 1 }
    );
    assert_eq!(
        log.get_operation(txn, 3).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone()
        }
    );

    // Delete the record.
    log.delete(txn, key, record_meta, 3).unwrap();
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
    assert_eq!(log.next_operation_id(txn).unwrap(), 5);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0],
    );
    assert!(!log.contains_operation_id(txn, append_only, 3).unwrap());
    assert_eq!(
        log.get_operation(txn, 4).unwrap().unwrap(),
        Operation::Delete { operation_id: 3 }
    );

    // Insert with that record id again.
    record_meta = log.insert_deleted(txn, key, &record, record_meta).unwrap();
    assert_eq!(log.count_present_records(txn, append_only).unwrap(), 2);
    assert_eq!(log.next_operation_id(txn).unwrap(), 6);
    assert_eq!(
        log.present_operation_ids(txn, append_only)
            .unwrap()
            .map(|result| result.map(IntoOwned::into_owned))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0, 5]
    );
    assert!(log.contains_operation_id(txn, append_only, 5).unwrap());
    assert_eq!(
        log.get_record_by_operation_id_unchecked(txn, 5).unwrap(),
        CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
    );
    assert_eq!(
        log.get_operation(txn, 5).unwrap().unwrap(),
        Operation::Insert {
            record_meta,
            record: record.clone(),
        }
    );
}
