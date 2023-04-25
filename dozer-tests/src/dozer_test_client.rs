use clap::Parser;
use dozer_types::tracing::info;
use std::{thread::sleep, time::Duration};

use dozer_tests::e2e_tests::{run_test_client, Case, CaseKind};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    case_dir: String,
    #[arg(long)]
    ignored: bool,
    #[arg(long)]
    connections_dir: String,
    #[arg(short, long)]
    dozer_api_host: String,
    #[arg(short, long)]
    wait_in_millis: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    let case = Case::load_from_case_dir(args.case_dir.clone().into(), args.connections_dir.into());
    if !args.ignored && case.should_be_ignored() {
        info!("Ignoring case {:?}", args.case_dir);
        return;
    }

    let CaseKind::Expectations(expectations) = case.kind else {
        panic!("Expectations not found in case {}", args.case_dir)
    };

    sleep(Duration::from_millis(args.wait_in_millis));

    // Modify dozer config to route to the dozer API host.
    let mut dozer_config = case.dozer_config;
    let mut api = dozer_config.api.unwrap_or_default();
    let mut rest = api.rest.unwrap_or_default();
    rest.host = args.dozer_api_host.clone();
    api.rest = Some(rest);
    let mut grpc = api.grpc.unwrap_or_default();
    grpc.host = args.dozer_api_host;
    api.grpc = Some(grpc);
    dozer_config.api = Some(api);
    run_test_client(dozer_config, &expectations).await;
}
