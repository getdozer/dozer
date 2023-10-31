use clap::Parser;
use dozer_log::reader::LogReaderBuilder;

#[derive(Parser)]
struct Cli {
    server_addr: String,
    endpoint: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let mut log_reader = LogReaderBuilder::new(cli.server_addr, cli.endpoint, Default::default())
        .await
        .unwrap()
        .build(0);

    let mut counter = 0;
    loop {
        log_reader.read_one().await.unwrap();
        counter += 1;

        if counter > 100000 {
            break;
        }
    }
}
