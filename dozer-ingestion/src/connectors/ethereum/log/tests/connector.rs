use dozer_types::types::{Field, Operation};
use hex_literal::hex;

use crate::test_util::create_test_runtime;

use super::helper::run_eth_sample;

#[test]
#[ignore]
fn test_eth_iterator() {
    let runtime = create_test_runtime();
    let wss_url = "ws://localhost:8545".to_string();
    let my_account = hex!("b49B3BEE604eF76410E84C7C98bC20335FdA0f75").into();

    let validate = |op: &Operation, idx: usize, field: Option<Field>| {
        if let Operation::Insert { new } = op {
            assert_eq!(new.values.get(idx), field.as_ref());
        } else {
            panic!("expected insert");
        }
    };

    let (contract, msgs) =
        runtime
            .clone()
            .block_on(run_eth_sample(runtime.clone(), wss_url, my_account));

    let address = format!("{:?}", contract.address());
    validate(&msgs[0], 1, Some(Field::String(address)));

    validate(&msgs[1], 0, Some(Field::String(format!("{my_account:?}"))));

    validate(&msgs[1], 1, Some(Field::String("Hello World!".to_string())));

    validate(&msgs[2], 0, None);
}
