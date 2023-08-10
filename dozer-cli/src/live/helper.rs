use dozer_api::grpc::types_helper::field_to_prost_value;
use dozer_types::grpc_types::{
    types::Record,
    types::{Operation, OperationType},
};

pub fn map_operation(endpoint_name: String, op: dozer_types::types::Operation) -> Operation {
    match op {
        dozer_types::types::Operation::Delete { old } => map_delete_operation(endpoint_name, old),
        dozer_types::types::Operation::Insert { new } => map_insert_operation(endpoint_name, new),
        dozer_types::types::Operation::Update { old, new } => {
            map_update_operation(endpoint_name, old, new)
        }
    }
}

fn map_record(record: dozer_types::types::Record) -> Record {
    Record {
        values: record
            .values
            .iter()
            .map(|f| field_to_prost_value(f.clone()))
            .collect(),
        version: 1,
    }
}

pub fn map_insert_operation(
    endpoint_name: String,
    record: dozer_types::types::Record,
) -> Operation {
    Operation {
        typ: OperationType::Insert as i32,
        old: None,
        new_id: None,
        new: Some(map_record(record)),
        endpoint_name,
    }
}

pub fn map_delete_operation(
    endpoint_name: String,
    record: dozer_types::types::Record,
) -> Operation {
    Operation {
        typ: OperationType::Delete as i32,
        old: None,
        new: Some(map_record(record)),
        new_id: None,
        endpoint_name,
    }
}

pub fn map_update_operation(
    endpoint_name: String,
    old: dozer_types::types::Record,
    new: dozer_types::types::Record,
) -> Operation {
    Operation {
        typ: OperationType::Update as i32,
        old: Some(map_record(old)),
        new: Some(map_record(new)),
        new_id: None,
        endpoint_name,
    }
}
