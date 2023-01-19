use tempdir::TempDir;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema};
use crate::dag::record_store::PrimaryKeyLookupRecordWriter;
use crate::storage::lmdb_storage::LmdbEnvironmentManager;

#[test]
fn test_pk_record_writer() {


    let tmp_path = TempDir::new("rw");
    let mut env = LmdbEnvironmentManager::create(tmp_path.expect("UNKNOWN").path(), "test").unwrap();
    let master_db = env.open_database("master", false).unwrap();
    let meta_db = env.open_database("meta", false).unwrap();
    let tx = env.create_txn().unwrap();

    let schema = Schema::empty()
        .field(FieldDefinition::new("id".to_string(), FieldType::Int, false), true)
        .field(FieldDefinition::new("name".to_string(), FieldType::String, false), false).clone();

    let writer = PrimaryKeyLookupRecordWriter::new(master_db, meta_db, schema.clone(), true, true);

    let input_record = Record::new(None, vec![Field::Int(1), Field::String("John1".to_string())], None);
    let input_key = input_record.get_key(&schema.primary_index);

    // No record inserted yet -> Must return an error
    assert!(writer.get_last_record_version(&input_key, &tx).is_err());

    writer.write_versioned_record(Some(&input_record), input_key.clone(), 1, &schema, &tx).unwrap();

    let ver = writer.get_last_record_version(&input_key, &tx).unwrap();
    let res_record = writer.retr_versioned_record(input_key.clone(), ver, &tx).unwrap();
    assert_eq!(res_record, Some(input_record));


    // Insert new version
    let input_record = Record::new(None, vec![Field::Int(1), Field::String("John2".to_string())], None);
    let input_key = input_record.get_key(&schema.primary_index);

    writer.write_versioned_record(Some(&input_record), input_key.clone(), 2, &schema, &tx).unwrap();

    let ver = writer.get_last_record_version(&input_key, &tx).unwrap();
    let res_record = writer.retr_versioned_record(input_key.clone(), ver, &tx).unwrap();
    assert_eq!(res_record, Some(input_record));

    // Retrieve old version
    let res_record = writer.retr_versioned_record(input_key.clone(), 1, &tx).unwrap();
    assert_eq!(res_record, Some(Record::new(None, vec![Field::Int(1), Field::String("John1".to_string())], None)));

    // Delete last version
    writer.write_versioned_record(None, input_key.clone(), 3, &schema, &tx).unwrap();
    let ver = writer.get_last_record_version(&input_key, &tx).unwrap();
    let res_record = writer.retr_versioned_record(input_key.clone(), ver, &tx).unwrap();
    assert!(res_record.is_none());

    let res_record = writer.retr_versioned_record(input_key.clone(), 1, &tx).unwrap();
    assert_eq!(res_record, Some(Record::new(None, vec![Field::Int(1), Field::String("John1".to_string())], None)));

    let res_record = writer.retr_versioned_record(input_key.clone(), 2, &tx).unwrap();
    assert_eq!(res_record, Some(Record::new(None, vec![Field::Int(1), Field::String("John2".to_string())], None)));

    
}