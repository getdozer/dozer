use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema, SchemaIdentifier,
};
use std::collections::HashMap;
use std::sync::Arc;

use web3::ethabi::{Contract, RawLog};
use web3::transports::WebSocket;
use web3::types::Log;

use crate::connectors::TableInfo;

use super::connector::ETH_LOGS_TABLE;
use super::sender::EthDetails;

pub async fn get_wss_client(url: &str) -> Result<web3::Web3<WebSocket>, web3::Error> {
    Ok(web3::Web3::new(
        web3::transports::WebSocket::new(url).await?,
    ))
}

pub fn get_contract_event_schemas(
    contract: &Contract,
    schema_map: HashMap<String, usize>,
) -> Vec<(String, Schema)> {
    let mut schemas = vec![];
    for event in contract.events.values().flatten() {
        let mut fields = vec![];
        for input in event.inputs.iter().cloned() {
            fields.push(FieldDefinition {
                name: input.name,
                typ: match input.kind {
                    web3::ethabi::ParamType::Address => FieldType::String,
                    web3::ethabi::ParamType::Bytes => FieldType::Binary,
                    web3::ethabi::ParamType::FixedBytes(_) => FieldType::Binary,
                    web3::ethabi::ParamType::Int(_) => FieldType::Int,
                    web3::ethabi::ParamType::Uint(_) => FieldType::UInt,
                    web3::ethabi::ParamType::Bool => FieldType::Boolean,
                    web3::ethabi::ParamType::String => FieldType::String,
                    // TODO: These are to be mapped to appropriate types
                    web3::ethabi::ParamType::Array(_)
                    | web3::ethabi::ParamType::FixedArray(_, _)
                    | web3::ethabi::ParamType::Tuple(_) => FieldType::Text,
                },
                nullable: false,
            });
        }

        let schema_id = schema_map
            .get(&event.name)
            .expect("schema is missing")
            .to_owned();

        schemas.push((
            event.name.to_owned(),
            Schema {
                identifier: Some(SchemaIdentifier {
                    id: schema_id as u32,
                    version: 1,
                }),
                fields,
                primary_index: vec![0],
            },
        ));
    }
    schemas
}

pub fn decode_event(
    log: Log,
    contract: Contract,
    tables: Option<Vec<TableInfo>>,
    schema_map: HashMap<String, usize>,
) -> Option<OperationEvent> {
    // Topics 0, 1, 2 should be name, buyer, seller in most cases
    let name = log
        .topics
        .get(0)
        .expect("name is expected")
        .to_owned()
        .to_string();
    let seq_no = get_id(&log) + 1;

    let event = contract
        .events
        .values()
        .flatten()
        .into_iter()
        .find(|evt| evt.signature().to_string() == name)
        .unwrap_or_else(|| panic!("event is not found with signature: {}", name));

    let schema_id = schema_map
        .get(&event.name)
        .expect("schema is missing")
        .to_owned();

    let is_table_required =
        tables.map_or(true, |tables| tables.iter().any(|t| t.name == event.name));
    if is_table_required {
        // let event = contract.event(&name_str).unwrap();
        let parsed_event = event
            .parse_log(RawLog {
                topics: log.topics,
                data: log.data.0,
            })
            .unwrap_or_else(|_| {
                panic!(
                    "parsing event failed: block_no: {}, txn_hash: {}",
                    log.block_number.unwrap(),
                    log.transaction_hash.unwrap()
                )
            });
        // info!("Event: {:?}", parsed_event);

        let values = parsed_event
            .params
            .into_iter()
            .map(|p| map_abitype_to_field(p.value))
            .collect();
        Some(OperationEvent {
            seq_no,
            operation: Operation::Insert {
                new: Record {
                    schema_id: Some(SchemaIdentifier {
                        id: schema_id as u32,
                        version: 1,
                    }),
                    values,
                },
            },
        })
    } else {
        None
    }
}
pub fn map_abitype_to_field(f: web3::ethabi::Token) -> Field {
    match f {
        web3::ethabi::Token::Address(f) => Field::String(format!("{:?}", f)),
        web3::ethabi::Token::FixedBytes(f) => Field::Binary(f),
        web3::ethabi::Token::Bytes(f) => Field::Binary(f),
        // TODO: Convert i64 appropriately
        web3::ethabi::Token::Int(f) => Field::Int(f.low_u32() as i64),
        web3::ethabi::Token::Uint(f) => Field::UInt(f.as_u64()),
        web3::ethabi::Token::Bool(f) => Field::Boolean(f),
        web3::ethabi::Token::String(f) => Field::String(f),
        web3::ethabi::Token::FixedArray(f)
        | web3::ethabi::Token::Array(f)
        | web3::ethabi::Token::Tuple(f) => Field::Text(
            f.iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(","),
        ),
    }
}
pub fn map_log_to_event(log: Log, details: Arc<EthDetails>) -> Option<OperationEvent> {
    // Check if table is requested
    let is_table_required = details.tables.as_ref().map_or(true, |tables| {
        tables.iter().any(|t| t.name == ETH_LOGS_TABLE)
    });

    if !is_table_required {
        None
    } else if log.log_index.is_some() {
        let (idx, values) = map_log_to_values(log);
        Some(OperationEvent {
            seq_no: idx,
            operation: Operation::Insert {
                new: Record {
                    schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
                    values,
                },
            },
        })
    } else {
        None
    }
}

pub fn get_id(log: &Log) -> u64 {
    let block_no = log
        .block_number
        .expect("expected for non pendning")
        .as_u64();

    let log_idx = log.log_index.expect("expected for non pendning").as_u64();

    block_no * 100_000 + log_idx * 2
}
pub fn map_log_to_values(log: Log) -> (u64, Vec<Field>) {
    let block_no = log
        .block_number
        .expect("expected for non pendning")
        .as_u64();
    let txn_idx = log
        .transaction_index
        .expect("expected for non pendning")
        .as_u64();
    let log_idx = log.log_index.expect("expected for non pendning").as_u64();

    let idx = get_id(&log);

    let values = vec![
        Field::Int(idx as i64),
        Field::String(log.address.to_string()),
        Field::Text(
            log.topics
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<String>>()
                .join(" "),
        ),
        Field::Binary(log.data.0),
        log.block_hash
            .map_or(Field::Null, |f| Field::String(f.to_string())),
        Field::UInt(block_no),
        log.transaction_hash
            .map_or(Field::Null, |f| Field::String(f.to_string())),
        Field::UInt(txn_idx),
        Field::UInt(log_idx),
        log.transaction_log_index
            .map_or(Field::Null, |f| Field::Int(f.try_into().unwrap())),
        log.log_type.map_or(Field::Null, Field::String),
        log.removed.map_or(Field::Null, Field::Boolean),
    ];

    (idx, values)
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

        primary_index: vec![0],
    }
}
