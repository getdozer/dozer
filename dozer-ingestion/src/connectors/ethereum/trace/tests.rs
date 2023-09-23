use std::{env, time::Duration};

use dozer_types::{
    ingestion_types::{EthTraceConfig, IngestionMessage},
    log::info,
    types::{Field, Operation},
};

use crate::{
    connectors::{
        ethereum::{helper, trace::helper::get_block_traces, EthTraceConnector},
        Connector,
    },
    test_util::{create_test_runtime, spawn_connector},
};

use super::connector::BatchIterator;

#[test]
fn test_iterator() {
    let mut iter = BatchIterator::new(1, Some(2), 1);
    assert_eq!(iter.next(), Some((1, 2)));
    assert_eq!(iter.next(), Some((2, 3)));
    assert_eq!(iter.next(), None);

    let mut iter = BatchIterator::new(1, Some(1), 3);
    assert_eq!(iter.next(), Some((1, 2)));
    assert_eq!(iter.next(), None);
}

#[tokio::test]
#[ignore]
async fn test_get_block_traces() {
    let url = env::var("ETH_HTTPS_URL").unwrap();
    let client = helper::get_batch_http_client(&url).await.unwrap();
    let traces = get_block_traces(client, (1000000, 1000005)).await.unwrap();
    assert!(!traces.is_empty(), "Failed to get traces found");
}

#[test]
#[ignore]
fn test_trace_iterator() {
    let runtime = create_test_runtime();
    let https_url = env::var("ETH_HTTPS_URL").unwrap();

    dozer_tracing::init_telemetry(None, &Default::default());
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
    }));

    info!("Initializing with WSS: {}", https_url);

    let connector = EthTraceConnector::new(
        EthTraceConfig {
            https_url,
            from_block: 1000000,
            to_block: Some(1000001),
            batch_size: Some(100),
        },
        "test".to_string(),
    );

    let (tables, schemas) = runtime.block_on(connector.list_all_schemas()).unwrap();
    for s in schemas {
        info!("\n{}", s.schema.print());
    }
    let (mut iterator, _) = spawn_connector(runtime.clone(), connector, tables);

    runtime.block_on(async move {
        if let Some(IngestionMessage::OperationEvent { op, .. }) =
            iterator.next_timeout(Duration::from_millis(1000)).await
        {
            assert!(matches!(op, Operation::Insert { .. }));
            if let Operation::Insert { new } = op {
                assert!(matches!(new.values[0], Field::String(_)));
                assert!(matches!(new.values[1], Field::String(_)));
                assert!(matches!(new.values[2], Field::String(_)));
                assert!(matches!(new.values[3], Field::UInt(_)));
                assert!(matches!(new.values[4], Field::UInt(_)));
                assert!(matches!(new.values[5], Field::UInt(_)));
                assert!(matches!(new.values[6], Field::Text(_)));
                assert!(matches!(new.values[7], Field::Text(_)));
            } else {
                panic!("Expected insert");
            }
        } else {
            panic!("No message received");
        }
    });
}
