use crate::pipeline::aggregation::processor::AggregationProcessor;
use dozer_types::types::Field;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[test]
fn encode_decode_buffer() {
    //
    //
    let prefix_0 = 100_u32;
    let field_0 = Field::Int(100);
    let state_0: Option<Vec<u8>> = Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let (_sz, mut buf) = chk!(AggregationProcessor::encode_buffer(
        prefix_0, &field_0, &state_0
    ));

    let prefix_1 = 200_u32;
    let field_1 = Field::Int(200);
    let state_1: Option<Vec<u8>> = Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    buf.extend(
        chk!(AggregationProcessor::encode_buffer(
            prefix_1, &field_1, &state_1
        ))
        .1,
    );
    let (sz, data) = chk!(AggregationProcessor::decode_buffer(&buf));

    assert_eq!(&data.state.unwrap(), &state_0.unwrap().as_slice());
    assert_eq!(data.value, field_0);
    assert_eq!(data.prefix, prefix_0);

    let (_sz, data) = chk!(AggregationProcessor::decode_buffer(&buf[sz..]));

    assert_eq!(&data.state.unwrap(), &state_1.unwrap().as_slice());
    assert_eq!(data.value, field_1);
    assert_eq!(data.prefix, prefix_1);
}
