use dozer_cache::cache::CacheRecord;
use dozer_types::grpc_types::conversions::field_to_grpc;
use dozer_types::grpc_types::types::{Operation, OperationType, Record, Value};

pub fn map_insert_operation(endpoint_name: String, record: CacheRecord) -> Operation {
    Operation {
        typ: OperationType::Insert as i32,
        old: None,
        new: Some(map_record(record)),
        endpoint_name,
    }
}

pub fn map_delete_operation(endpoint_name: String, record: CacheRecord) -> Operation {
    Operation {
        typ: OperationType::Delete as i32,
        old: None,
        new: Some(map_record(record)),
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
        old: Some(map_record(old)),
        new: Some(map_record(new)),
        endpoint_name,
    }
}

pub fn map_record(record: CacheRecord) -> Record {
    let values: Vec<Value> = record
        .record
        .values
        .into_iter()
        .map(field_to_grpc)
        .collect();

    Record {
        values,
        id: record.id,
        version: record.version,
    }
}
