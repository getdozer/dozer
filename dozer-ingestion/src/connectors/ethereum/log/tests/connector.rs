use dozer_types::types::{Field, Operation};
use hex_literal::hex;

use super::helper::run_eth_sample;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore]
async fn test_eth_iterator() {
    let wss_url = "ws://localhost:8545".to_string();
    let my_account = hex!("b49B3BEE604eF76410E84C7C98bC20335FdA0f75").into();

    let validate = |op: &Operation, idx: usize, field: Option<Field>| {
        if let Operation::Insert { new } = op {
            assert_eq!(new.values.get(idx), field.as_ref());
        } else {
            panic!("expected insert");
        }
    };

    let (contract, msgs) = run_eth_sample(wss_url, my_account).await;

    let address = format!("{:?}", contract.address());
    validate(&msgs[0], 1, Some(Field::String(address)));

    validate(&msgs[1], 0, Some(Field::String(format!("{my_account:?}"))));

    validate(&msgs[1], 1, Some(Field::String("Hello World!".to_string())));

    validate(&msgs[2], 0, None);
}
