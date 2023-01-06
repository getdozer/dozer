use std::borrow::Cow::{self, Borrowed, Owned};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::errors::{CliError, OrchestrationError};
use crate::utils::get_repl_history_path;
use crate::Orchestrator;
use dozer_cache::cache::index::get_primary_key;
use dozer_types::crossbeam::channel;
use dozer_types::log::{debug, error, info};
use dozer_types::prettytable::{Cell, Row, Table};
use dozer_types::types::{Field, Operation};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::{Hint, Hinter};
use rustyline::history::SearchDirection;
use rustyline::Editor;
use rustyline::{
    Cmd, ConditionalEventHandler, Context, Event, EventContext, EventHandler, KeyEvent, RepeatCount,
};

use rustyline_derive::{Completer, Helper, Validator};

use super::{init_dozer, list_sources, load_config};

#[derive(Completer, Helper, Validator)]
struct ConfigureHelper {
    // TODO: replace with radix tree
    hints: HashSet<CommandHint>,
}

#[derive(Hash, Debug, PartialEq, Eq)]
struct CommandHint {
    display: String,
    complete_up_to: usize,
}

impl Hint for CommandHint {
    fn display(&self) -> &str {
        &self.display
    }

    fn completion(&self) -> Option<&str> {
        if self.complete_up_to > 0 {
            Some(&self.display[..self.complete_up_to])
        } else {
            None
        }
    }
}

impl CommandHint {
    fn new(text: &str, complete_up_to: &str) -> CommandHint {
        assert!(text.starts_with(complete_up_to));
        CommandHint {
            display: text.into(),
            complete_up_to: complete_up_to.len(),
        }
    }

    fn suffix(&self, strip_chars: usize) -> CommandHint {
        CommandHint {
            display: self.display[strip_chars..].to_owned(),
            complete_up_to: self.complete_up_to.saturating_sub(strip_chars),
        }
    }
}

impl Hinter for ConfigureHelper {
    type Hint = CommandHint;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<CommandHint> {
        if line.is_empty() || pos < line.len() {
            return None;
        }

        let hint = self
            .hints
            .iter()
            .filter_map(|hint| {
                // expect hint after word complete, like redis cli, add condition:
                // line.ends_with(" ")
                if hint.display.starts_with(line) {
                    Some(hint.suffix(pos))
                } else {
                    None
                }
            })
            .next();

        if let Some(hint) = hint {
            Some(hint)
        } else {
            let start = if ctx.history_index() == ctx.history().len() {
                ctx.history_index().saturating_sub(1)
            } else {
                ctx.history_index()
            };
            if let Some(sr) = ctx
                .history()
                .starts_with(line, start, SearchDirection::Reverse)
            {
                if sr.entry == line {
                    return None;
                }
                let str = sr.entry[pos..].to_owned();
                let len = str.len();
                return Some(CommandHint {
                    display: str,
                    complete_up_to: len,
                });
            }
            None
        }
    }
}
impl Highlighter for ConfigureHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Owned(format!("\x1b[1;32m{prompt}\x1b[m"))
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned(format!("\x1b[1m{hint}\x1b[m"))
    }
}

fn hints() -> HashSet<CommandHint> {
    let mut set = HashSet::new();
    set.insert(CommandHint::new("help", "help"));
    set.insert(CommandHint::new("show sources", "show sources"));
    set.insert(CommandHint::new("SELECT col1 from table_name", "SELECT "));
    set
}

#[derive(Debug, Clone)]
enum DozerCmd {
    Help,
    ShowSources,
    Sql(String),
}
fn get_commands() -> HashMap<String, DozerCmd> {
    let mut map = HashMap::new();
    map.insert("help".to_string(), DozerCmd::Help);
    map.insert("show sources".to_string(), DozerCmd::ShowSources);

    map
}

fn query(
    sql: String,
    config_path: &String,
    running: Arc<AtomicBool>,
) -> Result<(), OrchestrationError> {
    let dozer = init_dozer(config_path.to_owned())?;
    let (sender, receiver) = channel::unbounded::<Operation>();

    let res = dozer.query(sql, sender, running.clone());
    match res {
        Ok(schema) => {
            let mut record_map: HashMap<Vec<u8>, Vec<Field>> = HashMap::new();
            let mut idx = 0;
            // Exit after 10 records
            let expected: usize = 10;
            // Wait for 500 * 100 timeout
            let mut times = 0;
            let instant = Instant::now();
            let mut last_shown = Duration::from_millis(0);
            let mut last_len = 0;

            let display = |record_map: &HashMap<Vec<u8>, Vec<Field>>| {
                let mut table = Table::new();

                // Fields Row

                let mut cells = vec![];
                for f in &schema.fields {
                    cells.push(Cell::new(&f.name));
                }
                table.add_row(Row::new(cells));

                for values in record_map.values() {
                    let mut cells = vec![];
                    for v in values {
                        let val_str = v.to_string().map_or("".to_string(), |v| v);
                        cells.push(Cell::new(&val_str));
                    }
                    table.add_row(Row::new(cells));
                }
                table.printstd();
            };
            display(&record_map);

            let pkey_index = if schema.primary_index.is_empty() {
                vec![0]
            } else {
                schema.primary_index
            };
            loop {
                let msg = receiver.recv_timeout(Duration::from_millis(100));
                if idx > 100 || record_map.len() >= expected || times > 500 {
                    break;
                }
                match msg {
                    Ok(msg) => {
                        match msg {
                            Operation::Delete { old } => {
                                let pkey = get_primary_key(&pkey_index, &old.values);
                                record_map.remove(&pkey);
                            }
                            Operation::Insert { new } => {
                                let pkey = get_primary_key(&pkey_index, &new.values);
                                record_map.insert(pkey, new.values);
                            }
                            Operation::Update { old, new } => {
                                let pkey = get_primary_key(&pkey_index, &old.values);
                                record_map.remove(&pkey);

                                let pkey = get_primary_key(&pkey_index, &new.values);
                                record_map.insert(pkey, new.values);
                            }
                        }
                        idx += 1;
                    }
                    Err(e) => match e {
                        channel::RecvTimeoutError::Timeout => {
                            times += 1;
                        }
                        channel::RecvTimeoutError::Disconnected => {
                            error!("channel disconnected");
                            break;
                        }
                    },
                }

                if instant.elapsed() - last_shown > Duration::from_millis(100) {
                    last_shown = instant.elapsed();

                    if last_len != record_map.len() {
                        display(&record_map);
                        last_len = record_map.len();
                    }
                }
            }
            display(&record_map);
            // Exit the pipeline
            running.store(false, Ordering::Relaxed);
        }

        Err(e) => {
            error!("{}", e);
        }
    }

    Ok(())
}
fn execute(
    cmd: &str,
    config_path: &String,
    running: Arc<AtomicBool>,
) -> Result<(), OrchestrationError> {
    let cmd_map = get_commands();
    let dozer_cmd = cmd_map.iter().find(|(s, _)| s.to_string() == *cmd);

    let dozer_cmd = dozer_cmd.map_or(DozerCmd::Sql(cmd.to_string()), |c| c.1.clone());

    match dozer_cmd {
        DozerCmd::Help => {
            print_help();
            Ok(())
        }
        DozerCmd::ShowSources => list_sources(config_path.to_owned()),
        DozerCmd::Sql(sql) => query(sql, config_path, running),
    }
}

fn print_help() {
    info!("Commands:");
    info!("");
    for (c, _) in get_commands() {
        info!("{}", c);
    }
    info!("");
    info!("(Or) SQL can be inputted, eg: SELECT * from users;");
}
struct TabEventHandler;
impl ConditionalEventHandler for TabEventHandler {
    fn handle(&self, evt: &Event, _: RepeatCount, _: bool, ctx: &EventContext) -> Option<Cmd> {
        debug_assert_eq!(*evt, Event::from(KeyEvent::from('\t')));
        if !ctx.has_hint() {
            return None; // default
        }
        evt.get(0).map(|_k| Cmd::CompleteHint)
    }
}

pub fn configure(config_path: String, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
    let config = load_config(config_path.clone())?;

    let h = ConfigureHelper { hints: hints() };
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
                if !line.is_empty() {
                    execute(&line, &config_path, running.clone())?;
                }
            }
            Err(ReadlineError::Interrupted) => {
                running.store(false, Ordering::SeqCst);
                info!("Exiting..");
                break;
            }
            Err(ReadlineError::Eof) => {
                running.store(false, Ordering::SeqCst);
                break;
            }
            Err(err) => {
                running.store(false, Ordering::SeqCst);
                error!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(&history_path)
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;
    running.store(false, Ordering::Relaxed);
    Ok(())
}
