use dozer_cache::cache::CacheRecord;
use dozer_types::grpc_types::conversions::field_to_grpc;
use dozer_types::grpc_types::types::{Operation, OperationType, Record, RecordWithId, Value};

pub fn map_insert_operation(endpoint_name: String, record: CacheRecord) -> Operation {
    Operation {
        typ: OperationType::Insert as i32,
        old: None,
        new_id: Some(record.id),
        new: Some(record_to_internal_record(record)),
        endpoint_name,
    }
}

pub fn map_delete_operation(endpoint_name: String, record: CacheRecord) -> Operation {
    Operation {
        typ: OperationType::Delete as i32,
        old: None,
        new: Some(record_to_internal_record(record)),
        new_id: None,
        endpoint_name,
    }
}

pub fn map_update_operation(
    endpoint_name: String,
    old: CacheRecord,
    new: CacheRecord,
) -> Operation {
    Operation {
        typ: OperationType::Update as i32,
        old: Some(record_to_internal_record(old)),
        new: Some(record_to_internal_record(new)),
        new_id: None,
        endpoint_name,
    }
}

fn record_to_internal_record(record: CacheRecord) -> Record {
    let values: Vec<Value> = record
        .record
        .values
        .into_iter()
        .map(field_to_grpc)
        .collect();

    Record {
        values,
        version: record.version,
    }
}

pub fn map_record(record: CacheRecord) -> RecordWithId {
    RecordWithId {
        id: record.id,
        record: Some(record_to_internal_record(record)),
    }
}
