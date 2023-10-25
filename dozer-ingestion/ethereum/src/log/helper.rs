use std::collections::HashMap;
use std::sync::Arc;

use dozer_ingestion_connector::dozer_types::log::error;
use dozer_ingestion_connector::dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};
use dozer_ingestion_connector::{CdcType, SourceSchema, TableToIngest};
use web3::ethabi::RawLog;
use web3::types::Log;

use super::connector::{ContractTuple, ETH_LOGS_TABLE};
use super::sender::EthDetails;

pub fn get_contract_event_schemas(
    contracts: &HashMap<String, ContractTuple>,
) -> Vec<(String, SourceSchema)> {
    let mut schemas = vec![];

    for contract_tuple in contracts.values() {
        for event in contract_tuple.0.events.values().flatten() {
            let mut fields = vec![];
            for input in event.inputs.iter().cloned() {
                fields.push(FieldDefinition {
                    name: input.name,
                    typ: match input.kind {
                        web3::ethabi::ParamType::Address => FieldType::String,
                        web3::ethabi::ParamType::Bytes => FieldType::Binary,
                        web3::ethabi::ParamType::FixedBytes(_) => FieldType::Binary,
                        web3::ethabi::ParamType::Int(_) => FieldType::UInt,
                        web3::ethabi::ParamType::Uint(_) => FieldType::UInt,
                        web3::ethabi::ParamType::Bool => FieldType::Boolean,
                        web3::ethabi::ParamType::String => FieldType::String,
                        // TODO: These are to be mapped to appropriate types
                        web3::ethabi::ParamType::Array(_)
                        | web3::ethabi::ParamType::FixedArray(_, _)
                        | web3::ethabi::ParamType::Tuple(_) => FieldType::Text,
                    },
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                });
            }

            schemas.push((
                get_table_name(contract_tuple, &event.name),
                SourceSchema::new(
                    Schema {
                        fields,
                        primary_index: vec![],
                    },
                    CdcType::Nothing,
                ),
            ));
        }
    }

    schemas
}

pub fn decode_event(
    log: Log,
    contracts: HashMap<String, ContractTuple>,
    tables: Vec<TableToIngest>,
) -> Option<(usize, Operation)> {
    let address = format!("{:?}", log.address);

    let mut c = contracts.get(&address);

    if c.is_none() {
        // match on wildcard
        let wild_card_contract = contracts.iter().find(|(k, _)| k.to_string() == *"*");
        c = wild_card_contract.map(|c| c.1);
    }
    if let Some(contract_tuple) = c {
        // Topics 0, 1, 2 should be name, buyer, seller in most cases
        let name = log
            .topics
            .get(0)
            .expect("name is expected")
            .to_owned()
            .to_string();
        let opt_event = contract_tuple
            .0
            .events
            .values()
            .flatten()
            .find(|evt| evt.signature().to_string() == name);

        if let Some(event) = opt_event {
            let table_name = get_table_name(contract_tuple, &event.name);
            let table_index = tables.iter().position(|t| t.name == table_name);
            if let Some(table_index) = table_index {
                let parsed_event = event.parse_log(RawLog {
                    topics: log.topics,
                    data: log.data.0,
                });

                match parsed_event {
                    Ok(parsed_event) => {
                        let values = parsed_event
                            .params
                            .into_iter()
                            .map(|p| map_abitype_to_field(p.value))
                            .collect();
                        return Some((
                            table_index,
                            Operation::Insert {
                                new: Record {
                                    values,
                                    lifetime: None,
                                },
                            },
                        ));
                    }
                    Err(_) => {
                        error!(
                            "parsing event failed: block_no: {}, txn_hash: {}. Have you included the right abi to address mapping ?",
                                    log.block_number.unwrap(),
                                    log.transaction_hash.unwrap()
                                );
                        return None;
                    }
                }
            }
        }
    }

    None
}

pub fn get_table_name(contract_tuple: &ContractTuple, event_name: &str) -> String {
    format!("{}_{}", contract_tuple.1, event_name)
}

pub fn map_abitype_to_field(f: web3::ethabi::Token) -> Field {
    match f {
        web3::ethabi::Token::Address(f) => Field::String(format!("{f:?}")),
        web3::ethabi::Token::FixedBytes(f) => Field::Binary(f),
        web3::ethabi::Token::Bytes(f) => Field::Binary(f),
        // TODO: Convert i64 appropriately
        web3::ethabi::Token::Int(f) => Field::UInt(f.low_u64()),
        web3::ethabi::Token::Uint(f) => Field::UInt(f.low_u64()),
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
pub fn map_log_to_event(log: Log, details: Arc<EthDetails>) -> Option<(usize, Operation)> {
    // Check if table is requested
    let table_index = details.tables.iter().position(|t| t.name == ETH_LOGS_TABLE);

    if let Some(table_index) = table_index {
        if log.log_index.is_some() {
            let values = map_log_to_values(log);
            Some((
                table_index,
                Operation::Insert {
                    new: Record {
                        values,
                        lifetime: None,
                    },
                },
            ))
        } else {
            None
        }
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
pub fn map_log_to_values(log: Log) -> Vec<Field> {
    let block_no = log.block_number.expect("expected for non pending").as_u64();
    let txn_idx = log
        .transaction_index
        .expect("expected for non pending")
        .as_u64();
    let log_idx = log.log_index.expect("expected for non pending").as_u64();

    let idx = get_id(&log);

    let values = vec![
        Field::UInt(idx),
        Field::String(format!("{:?}", log.address)),
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

    values
}

pub fn get_eth_schema() -> Schema {
    Schema {
        fields: vec![
            FieldDefinition {
                name: "id".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "address".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "topics".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "data".to_string(),
                typ: FieldType::Binary,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "block_hash".to_string(),
                typ: FieldType::String,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "block_number".to_string(),
                typ: FieldType::UInt,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "transaction_hash".to_string(),
                typ: FieldType::String,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "transaction_index".to_string(),
                typ: FieldType::Int,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "log_index".to_string(),
                typ: FieldType::Int,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "transaction_log_index".to_string(),
                typ: FieldType::Int,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "log_type".to_string(),
                typ: FieldType::String,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "removed".to_string(),
                typ: FieldType::Boolean,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
        ],

        primary_index: vec![0],
    }
}
