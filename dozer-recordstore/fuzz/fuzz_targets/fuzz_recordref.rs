#![no_main]

use dozer_recordstore::{ProcessorRecordStore, RecordRef};
use dozer_types::types::Field;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: Vec<Vec<Field>>| {
    let store = ProcessorRecordStore::new().unwrap();
    for record in data {
        let record_ref = store.create_ref(&record).unwrap();
        let fields: Vec<_> = store
            .load_ref(&record_ref)
            .unwrap()
            .into_iter()
            .map(|field| field.cloned())
            .collect();
        assert_eq!(fields, record);

        let cloned = record_ref.clone();

        drop(record_ref);

        // Check that dropping is sound (e.g. no double-free when a clone is dropped)
        let fields: Vec<_> = cloned
            .load()
            .into_iter()
            .map(|field| field.cloned())
            .collect();
        assert_eq!(fields, record);
    }
});
