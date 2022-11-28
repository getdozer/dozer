use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema, SchemaIdentifier,
};

use web3::transports::WebSocket;
use web3::types::Log;

pub async fn get_client(url: &str) -> Result<web3::Web3<WebSocket>, web3::Error> {
    Ok(web3::Web3::new(
        web3::transports::WebSocket::new(url).await?,
    ))
}

pub fn map_log_to_event(log: Log, idx: usize) -> OperationEvent {
    OperationEvent {
        seq_no: idx as u64,
        operation: Operation::Insert {
            new: Record {
                schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
                values: vec![
                    Field::Int(idx as i64),
                    Field::String(log.address.to_string()),
                    Field::Text(
                        log.topics
                            .iter()
                            .map(|t| t.to_string())
                            .collect::<Vec<String>>()
                            .join(" "),
                    ),
                    // Field::Binary(log.data),
                    Field::Binary(vec![]),
                    log.block_hash
                        .map_or(Field::Null, |f| Field::String(f.to_string())),
                    log.block_number
                        .map_or(Field::Null, |f| Field::Int(f.try_into().unwrap())),
                    log.transaction_hash
                        .map_or(Field::Null, |f| Field::String(f.to_string())),
                    log.transaction_index
                        .map_or(Field::Null, |f| Field::Int(f.try_into().unwrap())),
                    log.log_index
                        .map_or(Field::Null, |f| Field::Int(f.try_into().unwrap())),
                    log.transaction_log_index
                        .map_or(Field::Null, |f| Field::Int(f.try_into().unwrap())),
                    log.log_type.map_or(Field::Null, Field::String),
                    log.removed.map_or(Field::Null, Field::Boolean),
                ],
            },
        },
    }
}

pub fn get_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "address".to_string(),
        "topics".to_string(),
        "data".to_string(),
        "block_hash".to_string(),
        "block_number".to_string(),
        "transaction_hash".to_string(),
        "transaction_index".to_string(),
        "log_index".to_string(),
        "transaction_log_index".to_string(),
        "log_type".to_string(),
        "removed".to_string(),
    ]
}
pub fn get_eth_schema() -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: vec![
            FieldDefinition {
                name: "id".to_string(),
                typ: FieldType::Int,
                nullable: false,
            },
            FieldDefinition {
                name: "address".to_string(),
                typ: FieldType::String,
                nullable: false,
            },
            FieldDefinition {
                name: "topics".to_string(),
                typ: FieldType::String,
                nullable: false,
            },
            FieldDefinition {
                name: "data".to_string(),
                typ: FieldType::Binary,
                nullable: false,
            },
            FieldDefinition {
                name: "block_hash".to_string(),
                typ: FieldType::String,
                nullable: true,
            },
            FieldDefinition {
                name: "block_number".to_string(),
                typ: FieldType::Int,
                nullable: true,
            },
            FieldDefinition {
                name: "transaction_hash".to_string(),
                typ: FieldType::String,
                nullable: true,
            },
            FieldDefinition {
                name: "transaction_index".to_string(),
                typ: FieldType::Int,
                nullable: true,
            },
            FieldDefinition {
                name: "log_index".to_string(),
                typ: FieldType::Int,
                nullable: true,
            },
            FieldDefinition {
                name: "transaction_log_index".to_string(),
                typ: FieldType::Int,
                nullable: true,
            },
            FieldDefinition {
                name: "log_type".to_string(),
                typ: FieldType::String,
                nullable: true,
            },
            FieldDefinition {
                name: "removed".to_string(),
                typ: FieldType::Boolean,
                nullable: true,
            },
        ],
        values: vec![],
        // Log Index
        primary_index: vec![0],
    }
}
