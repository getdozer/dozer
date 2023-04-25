use std::path::Path;

use clap::Parser;
use dozer_tests::e2e_tests::{create_runner, Case, RunnerType};
use dozer_types::log::info;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "dozer-tests/src/e2e_tests/cases")]
    cases_dir: String,
    #[arg(long)]
    ignored: bool,
    #[arg(long, default_value = "dozer-tests/src/e2e_tests/connections")]
    connections_dir: String,
    #[arg(short, long, default_value_t = RunnerType::Local)]
    runner: RunnerType,
    #[arg(default_value = "")]
    case_prefix: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();
    let connections_dir = AsRef::<Path>::as_ref(&args.connections_dir)
        .canonicalize()
        .unwrap_or_else(|e| {
            panic!(
                "Failed to canonicalize connections path {}: {}",
                args.connections_dir, e
            )
        });

    let runner = create_runner(args.runner);
    // Traverse cases path.
    let cases_dir = AsRef::<Path>::as_ref(&args.cases_dir)
        .canonicalize()
        .unwrap_or_else(|e| {
            panic!(
                "Failed to canonicalize cases path {}: {}",
                args.cases_dir, e
            )
        });
    for entry in cases_dir
        .read_dir()
        .unwrap_or_else(|e| panic!("Failed to read cases path {}: {}", args.cases_dir, e))
    {
        let entry = entry.expect("Failed to read case entry");
        let case_dir = entry.path();
        if case_dir.is_dir()
            && case_dir
                .file_name()
                .unwrap_or_else(|| panic!("Case dir {case_dir:?} doesn't have file name"))
                .to_str()
                .unwrap_or_else(|| panic!("Non-UTF8 path {case_dir:?}"))
                .starts_with(&args.case_prefix)
        {
            let case = Case::load_from_case_dir(case_dir.clone(), connections_dir.clone());
            if !args.ignored && case.should_be_ignored() {
                info!("Ignoring case {:?}", case_dir);
            } else {
                info!("Running case {:?}", case_dir);
                runner.run_test_case(&case).await;
            }
        }
    }
}
