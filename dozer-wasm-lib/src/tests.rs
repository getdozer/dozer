use crate::sink::MemorySink;
use dozer_api::grpc::types::{value, Record, Value};

#[test]
fn sink_insert_update() {
    let mut sink = MemorySink::new(vec![0]);

    let record = Record {
        values: vec![
            Value {
                value: Some(value::Value::UintValue(1)),
            },
            Value {
                value: Some(value::Value::UintValue(2)),
            },
        ],
        version: 1,
    };

    let mut new = record.clone();
    new.values[0].value = Some(value::Value::UintValue(2));

    sink.insert(&record);

    sink.update(&record, &new);

    let key = sink.get_primary_key(&new);
    assert_eq!(sink.records.get(&key).unwrap(), &new, "must be equal");
}
