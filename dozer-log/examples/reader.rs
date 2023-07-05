use std::env;

use dozer_log::reader::LogReader;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut path = ".dozer/pipeline/logs/trips";
    if args.len() == 2 {
        path = &args[1];
    };
    let mut log_reader = LogReader::new(path.as_ref(), "logs".to_string(), 0, None)
        .await
        .unwrap();

    let mut counter = 0;
    loop {
        log_reader.next_op().await;
        counter += 1;

        if counter > 100000 {
            break;
        }
    }
}
