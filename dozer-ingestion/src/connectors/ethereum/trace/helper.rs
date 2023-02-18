use dozer_types::serde;
use dozer_types::serde_json::{self, json};
use dozer_types::types::{
    Field, FieldType, Operation, Record, Schema, SchemaIdentifier, SourceDefinition,
};
use serde::{Deserialize, Serialize};

use dozer_types::types::FieldDefinition;

use web3::helpers::CallFuture;
use web3::transports::WebSocket;
use web3::types::{H160, U256, U64};
use web3::{Transport, Web3};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde", rename_all = "camelCase")]
pub struct TraceResult {
    pub result: Trace,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde", rename_all = "camelCase")]
pub struct Trace {
    #[serde(rename = "type")]
    pub type_field: String,
    pub from: H160,
    pub to: H160,
    pub value: U256,
    pub gas: U256,
    pub gas_used: U256,
    pub input: Option<String>,
    pub output: Option<String>,
    pub calls: Option<Vec<Trace>>,
}

pub fn get_block_traces(
    client: Web3<WebSocket>,
    block_no: u64,
) -> CallFuture<Vec<TraceResult>, <WebSocket as Transport>::Out> {
    CallFuture::new(client.transport().execute(
        "debug_traceBlockByNumber",
        vec![
            serde_json::to_value(U64::from(block_no)).unwrap(),
            json!({
                    "tracer": "callTracer"
                }
            ),
        ],
    ))
}

fn get_schema_id() -> SchemaIdentifier {
    SchemaIdentifier { id: 1, version: 1 }
}
pub fn get_trace_schema() -> Schema {
    Schema {
        identifier: Some(get_schema_id()),
        fields: vec![
            FieldDefinition {
                name: "type_field".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "from".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "to".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "value".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "gas".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "gas_used".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "input".to_string(),
                typ: FieldType::Text,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "output".to_string(),
                typ: FieldType::Text,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
        ],
        primary_index: vec![],
    }
}

pub fn map_trace_to_ops(trace: &Trace) -> Vec<Operation> {
    let mut ops = vec![];
    let op = Operation::Insert {
        new: Record {
            schema_id: Some(get_schema_id()),
            values: vec![
                Field::String(trace.type_field.clone()),
                Field::String(format!("{:?}", trace.from)),
                Field::String(format!("{:?}", trace.to)),
                Field::UInt(trace.value.low_u64()),
                Field::UInt(trace.gas.low_u64()),
                Field::UInt(trace.gas_used.low_u64()),
                Field::Text(format!("{:?}", trace.input)),
                Field::Text(format!("{:?}", trace.output)),
            ],
            version: None,
        },
    };
    ops.push(op);
    if let Some(calls) = &trace.calls {
        for call in calls {
            ops.append(&mut map_trace_to_ops(call));
        }
    }
    ops
}
