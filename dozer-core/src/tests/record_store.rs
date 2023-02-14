use crate::record_store::{
    AutogenRowKeyLookupRecordReader, AutogenRowKeyLookupRecordWriter, KeyExtractor,
    PrimaryKeyLookupRecordReader, PrimaryKeyLookupRecordWriter, RecordReader, RecordWriter,
};
use dozer_storage::{
    lmdb::DatabaseFlags,
    lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};
use tempdir::TempDir;

#[test]
fn test_pk_record_writer() {
    let tmp_path = TempDir::new("rw");
    let mut env = LmdbEnvironmentManager::create(
        tmp_path.expect("UNKNOWN").path(),
        "test",
        LmdbEnvironmentOptions::default(),
    )
    .unwrap();
    let master_db = env
        .create_database(Some("master"), Some(DatabaseFlags::empty()))
        .unwrap();
    let meta_db = env
        .create_database(Some("meta"), Some(DatabaseFlags::empty()))
        .unwrap();
    let tx = env.create_txn().unwrap();

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                "name".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let writer = PrimaryKeyLookupRecordWriter::new(
        master_db,
        meta_db,
        schema.clone(),
        true,
        true,
        1000,
        KeyExtractor::PrimaryKey(schema.clone()),
    );

    let input_record = Record::new(
        None,
        vec![Field::Int(1), Field::String("John1".to_string())],
        None,
    );
    let input_key = input_record.get_key(&schema.primary_index);

    // No record inserted yet -> Must return an error
    assert!(writer.get_last_record_version(&input_key, &tx).is_err());

    writer
        .write_versioned_record(Some(&input_record), input_key.clone(), 1, &schema, &tx)
        .unwrap();

    let ver = writer.get_last_record_version(&input_key, &tx).unwrap();
    let res_record = writer.retr_versioned_record(input_key, ver, &tx).unwrap();
    assert_eq!(res_record, Some(input_record));

    // Insert new version
    let input_record = Record::new(
        None,
        vec![Field::Int(1), Field::String("John2".to_string())],
        None,
    );
    let input_key = input_record.get_key(&schema.primary_index);

    writer
        .write_versioned_record(Some(&input_record), input_key.clone(), 2, &schema, &tx)
        .unwrap();

    let ver = writer.get_last_record_version(&input_key, &tx).unwrap();
    let res_record = writer
        .retr_versioned_record(input_key.clone(), ver, &tx)
        .unwrap();
    assert_eq!(res_record, Some(input_record));

    // Retrieve old version
    let res_record = writer
        .retr_versioned_record(input_key.clone(), 1, &tx)
        .unwrap();
    assert_eq!(
        res_record,
        Some(Record::new(
            None,
            vec![Field::Int(1), Field::String("John1".to_string())],
            None
        ))
    );

    // Delete last version
    writer
        .write_versioned_record(None, input_key.clone(), 3, &schema, &tx)
        .unwrap();
    let ver = writer.get_last_record_version(&input_key, &tx).unwrap();
    let res_record = writer
        .retr_versioned_record(input_key.clone(), ver, &tx)
        .unwrap();
    assert!(res_record.is_none());

    let res_record = writer
        .retr_versioned_record(input_key.clone(), 1, &tx)
        .unwrap();
    assert_eq!(
        res_record,
        Some(Record::new(
            None,
            vec![Field::Int(1), Field::String("John1".to_string())],
            None
        ))
    );

    let res_record = writer.retr_versioned_record(input_key, 2, &tx).unwrap();
    assert_eq!(
        res_record,
        Some(Record::new(
            None,
            vec![Field::Int(1), Field::String("John2".to_string())],
            None
        ))
    );
}

#[test]
fn test_read_write_kv() {
    let tmp_path = TempDir::new("rw");
    let mut env = LmdbEnvironmentManager::create(
        tmp_path.expect("UNKNOWN").path(),
        "test",
        LmdbEnvironmentOptions::default(),
    )
    .unwrap();
    let master_db = env
        .create_database(Some("master"), Some(DatabaseFlags::empty()))
        .unwrap();
    let meta_db = env
        .create_database(Some("meta"), Some(DatabaseFlags::empty()))
        .unwrap();
    let tx = env.create_txn().unwrap();

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                "name".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let mut writer = PrimaryKeyLookupRecordWriter::new(
        master_db,
        meta_db,
        schema.clone(),
        true,
        true,
        1000,
        KeyExtractor::PrimaryKey(schema.clone()),
    );

    let r = writer
        .write(
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![Field::Int(1), Field::String("John1".to_string())],
                    None,
                ),
            },
            &tx,
        )
        .unwrap();
    assert_eq!(
        r,
        Operation::Insert {
            new: Record::new(
                None,
                vec![Field::Int(1), Field::String("John1".to_string())],
                Some(1)
            )
        }
    );

    let r = writer
        .write(
            Operation::Update {
                old: Record::new(None, vec![Field::Int(1), Field::Null], None),
                new: Record::new(
                    None,
                    vec![Field::Int(1), Field::String("John2".to_string())],
                    None,
                ),
            },
            &tx,
        )
        .unwrap();

    assert_eq!(
        r,
        Operation::Update {
            old: Record::new(
                None,
                vec![Field::Int(1), Field::String("John1".to_string())],
                Some(1)
            ),
            new: Record::new(
                None,
                vec![Field::Int(1), Field::String("John2".to_string())],
                Some(2),
            ),
        }
    );

    let r = writer
        .write(
            Operation::Delete {
                old: Record::new(None, vec![Field::Int(1), Field::Null], None),
            },
            &tx,
        )
        .unwrap();

    assert_eq!(
        r,
        Operation::Delete {
            old: Record::new(
                None,
                vec![Field::Int(1), Field::String("John2".to_string())],
                Some(2)
            )
        }
    );

    let lookup_record = Record::new(None, vec![Field::Int(1), Field::Null], None);
    let lookup_key = lookup_record.get_key(&schema.primary_index);

    let reader = PrimaryKeyLookupRecordReader::new(tx, master_db);
    let r = reader.get(&lookup_key, 1).unwrap();
    assert_eq!(
        r,
        Some(Record::new(
            None,
            vec![Field::Int(1), Field::String("John1".to_string())],
            Some(1)
        ))
    );

    let r = reader.get(&lookup_key, 3).unwrap();
    assert!(r.is_none());
}

#[test]
fn test_read_write_incr() {
    let tmp_path = TempDir::new("rw");
    let mut env = LmdbEnvironmentManager::create(
        tmp_path.expect("UNKNOWN").path(),
        "test",
        LmdbEnvironmentOptions::default(),
    )
    .unwrap();
    let master_db = env
        .create_database(Some("master"), Some(DatabaseFlags::empty()))
        .unwrap();
    let meta_db = env
        .create_database(Some("meta"), Some(DatabaseFlags::empty()))
        .unwrap();
    let tx = env.create_txn().unwrap();

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "name".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "rowkey".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .clone();

    let mut writer = AutogenRowKeyLookupRecordWriter::new(master_db, meta_db, schema.clone());

    let r = writer
        .write(
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![Field::Int(1), Field::String("John1".to_string())],
                    None,
                ),
            },
            &tx,
        )
        .unwrap();
    assert_eq!(
        r,
        Operation::Insert {
            new: Record::new(
                None,
                vec![
                    Field::Int(1),
                    Field::String("John1".to_string()),
                    Field::UInt(1)
                ],
                Some(1)
            )
        }
    );

    let lookup_record = Record::new(
        None,
        vec![
            Field::Int(1),
            Field::String("John1".to_string()),
            Field::UInt(1),
        ],
        Some(1),
    );
    let lookup_key = lookup_record.get_key(&schema.primary_index);

    let reader = AutogenRowKeyLookupRecordReader::new(tx, master_db);
    let r = reader.get(&lookup_key, 1).unwrap();
    assert_eq!(r, Some(lookup_record));
}
