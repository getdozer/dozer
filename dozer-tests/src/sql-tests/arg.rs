use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct SqlLogicTestArgs {
    // Choose suits to run
    #[arg(
        short = 'u',
        long = "suites",
        help = "The tests to be run will come from under suits",
        default_value = "src/sql-tests/test_suits"
    )]
    pub suites: String,

    // If enable complete mode
    #[arg(
        short = 'c',
        long = "complete",
        default_missing_value = "true",
        help = "The arg is used to enable auto complete mode"
    )]
    pub complete: bool,
}
