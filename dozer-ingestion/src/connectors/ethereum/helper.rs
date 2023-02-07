use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record,
    ReplicationChangesTrackingType, Schema, SchemaIdentifier, SchemaWithChangesType,
    SourceDefinition,
};
use std::collections::HashMap;
use std::sync::Arc;

use web3::ethabi::RawLog;
use web3::transports::WebSocket;
use web3::types::{Log, H256};

use crate::connectors::TableInfo;

use super::connector::{ContractTuple, ETH_LOGS_TABLE};
use super::sender::EthDetails;

pub async fn get_wss_client(url: &str) -> Result<web3::Web3<WebSocket>, web3::Error> {
    Ok(web3::Web3::new(
        web3::transports::WebSocket::new(url).await?,
    ))
}

pub fn get_contract_event_schemas(
    contracts: HashMap<String, ContractTuple>,
    schema_map: HashMap<H256, usize>,
) -> Vec<SchemaWithChangesType> {
    let mut schemas = vec![];

    for (_, contract_tuple) in contracts {
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

            let schema_id = schema_map
                .get(&event.signature())
                .expect("schema is missing")
                .to_owned();

            schemas.push((
                get_table_name(&contract_tuple, &event.name),
                Schema {
                    identifier: Some(SchemaIdentifier {
                        id: schema_id as u32,
                        version: 1,
                    }),
                    fields,
                    primary_index: vec![0],
                },
                ReplicationChangesTrackingType::FullChanges,
            ));
        }
    }

    schemas
}

pub fn decode_event(
    log: Log,
    contracts: HashMap<String, ContractTuple>,
    tables: Option<Vec<TableInfo>>,
    schema_map: HashMap<H256, usize>,
) -> Option<OperationEvent> {
    let address = format!("{:?}", log.address);

    if let Some(contract_tuple) = contracts.get(&address) {
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
            .into_iter()
            .find(|evt| evt.signature().to_string() == name);

        if let Some(event) = opt_event {
            let schema_id = schema_map
                .get(&event.signature())
                .expect("schema is missing")
                .to_owned();

            let seq_no = get_id(&log) + schema_id as u64;
            let table_name = get_table_name(contract_tuple, &event.name);
            let is_table_required = tables.map_or(true, |tables| {
                tables.iter().any(|t| t.table_name == table_name)
            });
            if is_table_required {
                let parsed_event = event
                    .parse_log(RawLog {
                        topics: log.topics,
                        data: log.data.0,
                    })
                    .unwrap_or_else(|_| {
                        panic!(
                            "parsing event failed: block_no: {}, txn_hash: {}. Have you included the right abi to address mapping ?",
                            log.block_number.unwrap(),
                            log.transaction_hash.unwrap()
                        )
                    });

                // let columns_idx = get_columns_idx(&table_name, default_columns, tables.clone());
                let values = parsed_event
                    .params
                    .into_iter()
                    .map(|p| map_abitype_to_field(p.value))
                    .collect();
                return Some(OperationEvent {
                    seq_no,
                    operation: Operation::Insert {
                        new: Record {
                            schema_id: Some(SchemaIdentifier {
                                id: schema_id as u32,
                                version: 1,
                            }),
                            values,
                            version: None,
                        },
                    },
                });
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
pub fn map_log_to_event(log: Log, details: Arc<EthDetails>) -> Option<OperationEvent> {
    // Check if table is requested
    let is_table_required = details.tables.as_ref().map_or(true, |tables| {
        tables.iter().any(|t| t.table_name == ETH_LOGS_TABLE)
    });

    if !is_table_required {
        None
    } else if log.log_index.is_some() {
        let (idx, values) = map_log_to_values(log, details);
        Some(OperationEvent {
            seq_no: idx,
            operation: Operation::Insert {
                new: Record {
                    schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
                    values,
                    version: None,
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
pub fn map_log_to_values(log: Log, details: Arc<EthDetails>) -> (u64, Vec<Field>) {
    let block_no = log.block_number.expect("expected for non pending").as_u64();
    let txn_idx = log
        .transaction_index
        .expect("expected for non pending")
        .as_u64();
    let log_idx = log.log_index.expect("expected for non pending").as_u64();

    let idx = get_id(&log);

    let default_columns = get_eth_schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<String>>();

    let columns_idx = get_columns_idx(ETH_LOGS_TABLE, default_columns, details.tables.clone());

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

    let values = columns_idx
        .iter()
        .map(|idx| values[*idx].clone())
        .collect::<Vec<Field>>();
    (idx, values)
}

pub fn get_columns_idx(
    table_name: &str,
    default_columns: Vec<String>,
    tables: Option<Vec<TableInfo>>,
) -> Vec<usize> {
    let columns = tables.as_ref().map_or(vec![], |tables| {
        tables
            .iter()
            .find(|t| t.table_name == table_name)
            .map_or(vec![], |t| {
                t.columns.as_ref().map_or(vec![], |cols| cols.clone())
            })
    });
    let columns = if columns.is_empty() {
        default_columns.clone()
    } else {
        columns
    };

    columns
        .iter()
        .map(|c| {
            default_columns
                .iter()
                .position(|f| f == c)
                .expect(&format!("column not found: {}", c))
        })
        .collect::<Vec<usize>>()
}

pub fn get_eth_schema() -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
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
