use crate::pipeline::aggregation::processor::{AggregationData, AggregationProcessor};
use dozer_types::types::Field;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[test]
fn encode_decode_buffer() {
    let field = Field::Int(100);
    let state: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];

    let (mut sz, mut buf) = chk!(AggregationProcessor::encode_embedded_state(&field, &state));
    buf.extend(chk!(AggregationProcessor::encode_embedded_state(&field, &state)).1);

    let decoded = chk!(AggregationProcessor::decode_buffer(&buf));

    match decoded.1 {
        AggregationData::EmbeddedState {
            curr_value,
            curr_state,
        } => {
            assert_eq!(curr_state, state);
            assert_eq!(curr_value, field);
        }
        AggregationData::KeyValueState { .. } => {}
    }

    let decoded = chk!(AggregationProcessor::decode_buffer(&buf[decoded.0..]));

    match decoded.1 {
        AggregationData::EmbeddedState {
            curr_value,
            curr_state,
        } => {
            assert_eq!(curr_state, state);
            assert_eq!(curr_value, field);
        }
        AggregationData::KeyValueState { .. } => {}
    }
}
