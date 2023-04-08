use std::{env, path::Path};

use dozer_log::reader::LogReader;
use futures_util::StreamExt;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut path = ".dozer/pipeline/logs/trips";
    if args.len() == 2 {
        path = &args[1];
    };
    let log_reader = LogReader::new(Path::new(path), "logs", 0, None).unwrap();

    tokio::pin!(log_reader);

    let mut counter = 0;
    while let Some(_op) = log_reader.next().await {
        counter += 1;

        if counter > 100000 {
            break;
        }
    }
}
