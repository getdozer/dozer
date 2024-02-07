#![no_main]
use dozer_recordstore::{ProcessorRecordStore, StoreRecord};
use dozer_types::{models::app_config::RecordStore, types::Field};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: Vec<Field>| {
    let store = ProcessorRecordStore::new(RecordStore::InMemory).unwrap();
    let record_ref = store.create_ref(&data).unwrap();

    assert_eq!(store.load_ref(&record_ref).unwrap(), data);

    let cloned = record_ref.clone();

    drop(record_ref);
    assert_eq!(
        // Check that dropping is sound (e.g. no double-free when a clone is dropped)
        store.load_ref(&cloned).unwrap(),
        data
    );
});
