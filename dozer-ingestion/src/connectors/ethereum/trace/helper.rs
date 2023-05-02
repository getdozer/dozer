use dozer_types::log::{debug, error};
use dozer_types::serde;
use dozer_types::serde_json::{self, json};
use dozer_types::types::{
    Field, FieldType, Operation, Record, Schema, SchemaIdentifier, SourceDefinition,
};
use serde::{Deserialize, Serialize};

use dozer_types::types::FieldDefinition;
use web3::transports::{Batch, Http};
use web3::types::{H160, U256};
use web3::{BatchTransport, Transport, Web3};

use crate::errors::ConnectorError;

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
    pub value: Option<U256>,
    pub gas: U256,
    pub gas_used: U256,
    pub input: Option<String>,
    pub output: Option<String>,
    pub calls: Option<Vec<Trace>>,
}

pub async fn get_block_traces(
    tuple: (Web3<Batch<Http>>, Http),
    batch: (u64, u64),
) -> Result<Vec<TraceResult>, ConnectorError> {
    debug_assert!(batch.0 < batch.1, "Batch start must be less than batch end");
    let (client, transport) = tuple;
    let mut requests = vec![];
    let mut results = vec![];
    debug!("Getting eth traces for block range: {:?}", batch);
    let mut request_count = 0;
    let (from, to) = batch;
    for block_no in from..to {
        let request = client.transport().prepare(
            "debug_traceBlockByNumber",
            vec![
                format!("0x{block_no:x}").into(),
                json!({
                        "tracer": "callTracer"
                    }
                ),
            ],
        );
        requests.push(request);
        request_count += 1;
    }

    let batch_results = transport
        .send_batch(requests)
        .await
        .map_err(ConnectorError::EthError)?;

    debug!(
        "Requests: {:?}, Results: {:?}",
        request_count,
        batch_results.len(),
    );

    for (idx, res) in batch_results.iter().enumerate() {
        let res = res.clone().map_err(|e| {
            error!("Error getting trace: {:?}", e);
            ConnectorError::EthError(e)
        })?;

        let r: Vec<TraceResult> =
            serde_json::from_value(res).map_err(ConnectorError::map_serialization_error)?;

        debug!("Idx: {} : Response: {:?}", idx, r);

        results.extend(r);
    }
    Ok(results)
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
                Field::UInt(trace.value.unwrap_or(U256::zero()).low_u64()),
                Field::UInt(trace.gas.low_u64()),
                Field::UInt(trace.gas_used.low_u64()),
                Field::Text(format!("{:?}", trace.input)),
                Field::Text(format!("{:?}", trace.output)),
            ],
            lifetime: None,
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
