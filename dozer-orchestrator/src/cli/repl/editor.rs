use crate::cli::repl::helper::get_commands;
use crate::cli::{list_sources, load_config};
use crate::errors::{CliError, OrchestrationError};
use crate::utils::get_repl_history_path;

use dozer_types::log::{debug, error, info};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use rustyline::{EventHandler, KeyEvent};

use super::helper::{ConfigureHelper, DozerCmd, TabEventHandler};
use super::query;

pub fn configure(config_path: String) -> Result<(), OrchestrationError> {
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
                if !line.is_empty() && !execute(&line, &config_path)? {
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

fn execute(cmd: &str, config_path: &String) -> Result<bool, OrchestrationError> {
    let cmd_map = get_commands();
    let dozer_cmd = cmd_map.iter().find(|(s, _)| s.to_string() == *cmd);

    let dozer_cmd = dozer_cmd.map_or(DozerCmd::Sql(cmd.to_string()), |c| c.1.clone());

    match dozer_cmd {
        DozerCmd::Help => {
            print_help();
            Ok(true)
        }
        DozerCmd::ShowSources => {
            list_sources(config_path.to_owned())?;
            Ok(true)
        }
        DozerCmd::Sql(sql) => {
            query(sql, config_path)?;
            Ok(true)
        }
        DozerCmd::Exit => Ok(false),
    }
}

fn print_help() {
    println!("Commands:");
    println!("");
    for (c, _) in get_commands() {
        info!("{}", c);
    }
    info!("");
    info!("(Or) SQL can be inputted, eg: SELECT * from users;");
}
