use std::{env, thread, time::Duration};

use dozer_types::{
    ingestion_types::EthTraceConfig,
    log::info,
    types::{Field, Operation},
};

use crate::{
    connectors::{
        ethereum::{helper, trace::helper::get_block_traces, EthTraceConnector},
        Connector,
    },
    ingestion::{IngestionConfig, Ingestor},
};

#[tokio::test]
#[ignore]
async fn test_get_block_traces() {
    let url = env::var("ETH_WSS_URL").unwrap();
    let client = helper::get_wss_client(&url).await.unwrap();
    let traces = get_block_traces(client, 1000000).await.unwrap();
    println!("{:?}", traces);
    assert!(traces.len() > 0, "Failed to get traces found");
}

#[test]
#[ignore]
fn test_trace_iterator() {
    let wss_url = env::var("ETH_WSS_URL").unwrap();

    dozer_tracing::init_telemetry(false).unwrap();
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
    }));

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    let _t = thread::spawn(move || {
        info!("Initializing with WSS: {}", wss_url);

        let connector = EthTraceConnector::new(
            1,
            wss_url,
            EthTraceConfig {
                from_block: 1000000,
                to_block: Some(1000001),
            },
        );

        let schemas = connector.get_schemas(None).unwrap();
        for (_, schema, _) in schemas {
            schema.print().printstd();
        }
        connector.start(None, &ingestor, None).unwrap();
    });

    if let Some((_, op)) = iterator.next_timeout(Duration::from_millis(1000)) {
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
}
