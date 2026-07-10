use dozer_cli::cli::{run, Args};
use dozer_cli::config_helper::load_config;
use dozer_cli::console_helper::setup_logging;
use dozer_cli::errors::DozerError;
use dozer_cli::home_dir::get_home_dir;
use dozer_cli::pipeline::Pipeline;
use dozer_cli::simple::Simple;
use dozer_cli::ui::UI;
use dozer_cli::utils::get_config_path;

fn main() -> Result<(), DozerError> {
    let args = Args::parse();
    let config_path = get_config_path(&args.config)?;
    let config = load_config(&config_path)?;
    setup_logging(&config.log_level)?;
    let ui = UI::new(&config)?;
    let pipeline = Pipeline::new(&config)?;
    let simple = Simple::new(&config)?;
    run(&args, &config, &ui, &pipeline, &simple)?;
    Ok(())
}
