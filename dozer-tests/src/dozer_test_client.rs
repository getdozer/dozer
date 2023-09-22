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
    let api = &mut dozer_config.api;
    let rest = &mut api.rest;
    rest.host = Some(args.dozer_api_host.clone());
    let grpc = &mut api.grpc;
    grpc.host = Some(args.dozer_api_host);
    run_test_client(dozer_config, &expectations).await;
}
