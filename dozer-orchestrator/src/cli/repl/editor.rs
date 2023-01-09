use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::cli::repl::helper::get_commands;
use crate::cli::{list_sources, load_config};
use crate::errors::{CliError, OrchestrationError};
use crate::utils::get_repl_history_path;

use super::helper::{ConfigureHelper, DozerCmd, TabEventHandler};
use dozer_types::log::{debug, error, info};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use rustyline::{EventHandler, KeyEvent};

pub fn configure(config_path: String, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
    let config = load_config(config_path.clone())?;

    let h = ConfigureHelper {
        hints: super::helper::hints(),
    };
    let mut rl = Editor::<ConfigureHelper>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    rl.set_helper(Some(h));
    rl.bind_sequence(
        KeyEvent::from('\t'),
        EventHandler::Conditional(Box::new(TabEventHandler)),
    );
    let history_path = get_repl_history_path(&config);

    if rl.load_history(history_path.as_path()).is_err() {
        debug!("No previous history file found.");
    }
    loop {
        let readline = rl.readline("dozer> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if !line.is_empty() && !execute(&line, &config_path, running.clone())? {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) => {
                info!("Exiting..");
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                error!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(&history_path)
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    Ok(())
}

fn execute(
    cmd: &str,
    config_path: &String,
    running: Arc<AtomicBool>,
) -> Result<bool, OrchestrationError> {
    let cmd_map = get_commands();
    let (_, dozer_cmd) = cmd_map
        .iter()
        .find(|(s, _)| s.to_string() == *cmd)
        .map_or(Err(CliError::UnknownCommand(cmd.to_string())), Ok)?;

    match dozer_cmd {
        DozerCmd::Help => {
            print_help();
            Ok(true)
        }
        DozerCmd::ShowSources => {
            list_sources(config_path)?;
            Ok(true)
        }
        DozerCmd::Sql => {
            super::sql::editor(config_path, running)?;
            Ok(true)
        }
        DozerCmd::Exit => Ok(false),
        DozerCmd::TestConnections => todo!(),
    }
}

fn print_help() {
    use std::println as info;
    info!("Commands:");
    info!();
    for (c, _) in get_commands() {
        info!("{}", c);
    }
    info!();
}
