use clap::Parser;
use dozer_log::reader::{LogReaderBuilder, LogReaderOptions};

#[derive(Parser)]
struct Cli {
    server_addr: String,
    endpoint: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let mut log_reader =
        LogReaderBuilder::new(cli.server_addr, LogReaderOptions::new(cli.endpoint))
            .await
            .unwrap()
            .build(0, None);

    let mut counter = 0;
    loop {
        log_reader.next_op().await.unwrap();
        counter += 1;

        if counter > 100000 {
            break;
        }
    }
}
