use std::path::Path;

use dozer_api::LogReader;
use futures_util::StreamExt;

#[tokio::main]
async fn main() {
    let path = ".dozer/pipeline/logs/trips";
    let log_reader = LogReader::new(Path::new(path), "trips", 0, None).unwrap();

    tokio::pin!(log_reader);

    let mut counter = 0;
    while let Some(_op) = log_reader.next().await {
        counter += 1;

        if counter > 100000 {
            break;
        }
    }
}
