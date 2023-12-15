use std::time::Duration;

use camino::Utf8Path;
use dozer_ingestion_connector::{test_util::create_test_runtime, IngestionConfig, Ingestor};

use super::JsExtension;

#[test]
#[ignore = "this test fails if it runs together with tests in `dozer-deno`. Not sure why."]
fn test_deno() {
    let js_path = Utf8Path::new(env!("CARGO_MANIFEST_DIR")).join("./src/js_extension/ingest.js");

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    let runtime = create_test_runtime();
    let ext = JsExtension::new(runtime.clone(), ingestor, js_path.into()).unwrap();

    runtime.spawn(ext.run());

    runtime.block_on(async move {
        let mut count = 0;
        loop {
            let msg = iterator.next_timeout(Duration::from_secs(5)).await.unwrap();
            count += 1;
            println!("i: {:?}, msg: {:?}", count, msg);
            if count > 3 {
                break;
            }
        }
    });
}
