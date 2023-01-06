use std::path::Path;

use clap::Parser;
use dozer_tests::e2e_tests::{create_runner, Case, RunnerType};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "dozer-tests/src/e2e_tests/cases")]
    cases_path: String,
    #[arg(short, long, default_value_t = RunnerType::Local)]
    runner: RunnerType,
    #[arg(default_value = "")]
    case_prefix: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    let runner = create_runner(args.runner);
    // Traverse cases path.
    let cases_path = AsRef::<Path>::as_ref(&args.cases_path)
        .canonicalize()
        .unwrap_or_else(|e| {
            panic!(
                "Failed to canonicalize cases path {}: {}",
                args.cases_path, e
            )
        });
    for entry in cases_path
        .read_dir()
        .unwrap_or_else(|e| panic!("Failed to read cases path {}: {}", args.cases_path, e))
    {
        let entry = entry.expect("Failed to read case entry");
        let case_dir = entry.path();
        if case_dir.is_dir()
            && case_dir
                .file_name()
                .unwrap_or_else(|| panic!("Case dir {:?} doesn't have file name", case_dir))
                .to_str()
                .unwrap_or_else(|| panic!("Non-UTF8 path {:?}", case_dir))
                .starts_with(&args.case_prefix)
        {
            let case = Case::load_from_case_dir(case_dir);
            runner.run_test_case(&case).await;
        }
    }
}
