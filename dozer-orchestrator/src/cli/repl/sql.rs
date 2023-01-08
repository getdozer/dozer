use crate::errors::CliError;
use crate::utils::get_sql_history_path;
use std::collections::HashMap;

use std::io::{stdout, Stdout, Write};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cli::{init_dozer, load_config};
use crate::errors::OrchestrationError;
use crate::Orchestrator;
use crossterm::{cursor, terminal, ExecutableCommand};
use dozer_cache::cache::index::get_primary_key;
use dozer_types::crossbeam::channel;
use dozer_types::log::{debug, error, info};
use dozer_types::prettytable::color;
use dozer_types::prettytable::{Cell, Row, Table};
use dozer_types::types::{Field, Operation, Schema};
use rustyline::error::ReadlineError;

use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

const HELP: &str = r#"
Enter your SQL below. 
Use semicolon at the end to run the query or Ctrl-C to cancel. 

"#;
#[derive(Completer, Helper, Highlighter, Hinter)]
struct SqlValidator {}

impl Validator for SqlValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        use ValidationResult::{Incomplete, Valid};
        let input = ctx.input();
        let result = if !input.ends_with(';') {
            Incomplete
        } else {
            Valid(None)
        };
        Ok(result)
    }
}

pub fn editor(config_path: &String, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
    use std::println as info;
    let h = SqlValidator {};
    let mut rl = rustyline::Editor::<SqlValidator>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;

    rl.set_helper(Some(h));
    let config = load_config(config_path.clone())?;

    let history_path = get_sql_history_path(&config);
    if rl.load_history(history_path.as_path()).is_err() {
        debug!("No previous history file found.");
    }

    let mut stdout = stdout();
    // let mut out = terminal::

    info!("{}", HELP);

    loop {
        let readline = rl.readline("sql>");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if !line.is_empty() {
                    stdout
                        .execute(cursor::Hide)
                        .map_err(CliError::TerminalError)?;
                    query(line, config_path, running.clone(), &mut stdout)?;
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
    stdout
        .execute(cursor::Show)
        .map_err(CliError::TerminalError)?;
    rl.save_history(&history_path)
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;

    Ok(())
}

pub fn query(
    sql: String,
    config_path: &String,
    running: Arc<AtomicBool>,
    stdout: &mut Stdout,
) -> Result<(), OrchestrationError> {
    let dozer = init_dozer(config_path.to_owned())?;
    let (sender, receiver) = channel::unbounded::<Operation>();

    // set running
    running.store(true, std::sync::atomic::Ordering::Relaxed);

    stdout
        .execute(cursor::SavePosition)
        .map_err(CliError::TerminalError)?;
    let res = dozer.query(sql, sender, running);
    match res {
        Ok(schema) => {
            let mut record_map: HashMap<Vec<u8>, Vec<Field>> = HashMap::new();
            let mut idx: u64 = 0;

            let instant = Instant::now();
            let mut last_shown = Duration::from_millis(0);
            let mut prev_len = 0;
            let pkey_index = if schema.primary_index.is_empty() {
                vec![]
            } else {
                schema.primary_index.clone()
            };

            let mut updates_map = HashMap::new();

            loop {
                let msg = receiver.recv_timeout(Duration::from_millis(100));

                match msg {
                    Ok(msg) => {
                        match msg {
                            Operation::Delete { old } => {
                                let pkey = if pkey_index.is_empty() {
                                    idx.to_le_bytes().to_vec()
                                } else {
                                    get_primary_key(&pkey_index, &old.values)
                                };
                                record_map.remove(&pkey);
                                updates_map.insert(pkey, (0, instant.elapsed()));
                            }
                            Operation::Insert { new } => {
                                let pkey = if pkey_index.is_empty() {
                                    idx.to_le_bytes().to_vec()
                                } else {
                                    get_primary_key(&pkey_index, &new.values)
                                };
                                record_map.insert(pkey.clone(), new.values);
                                updates_map.insert(pkey, (1, instant.elapsed()));
                            }
                            Operation::Update { old, new } => {
                                let pkey = if pkey_index.is_empty() {
                                    idx.to_le_bytes().to_vec()
                                } else {
                                    get_primary_key(&pkey_index, &old.values)
                                };
                                let pkey2 = if pkey_index.is_empty() {
                                    idx.to_le_bytes().to_vec()
                                } else {
                                    get_primary_key(&pkey_index, &new.values)
                                };
                                record_map.remove(&pkey);

                                record_map.insert(pkey2.clone(), new.values);
                                if pkey2 == pkey {
                                    updates_map.insert(pkey2, (2, instant.elapsed()));
                                } else {
                                    updates_map.insert(pkey2, (2, instant.elapsed()));
                                    updates_map.insert(pkey.clone(), (0, instant.elapsed()));
                                }
                            }
                        }
                        idx += 1;
                    }
                    Err(e) => match e {
                        channel::RecvTimeoutError::Timeout => {}
                        channel::RecvTimeoutError::Disconnected => {
                            break;
                        }
                    },
                }

                if instant.elapsed() - last_shown > Duration::from_millis(300) {
                    last_shown = instant.elapsed();
                    display(
                        stdout,
                        instant,
                        &schema,
                        &record_map,
                        &updates_map,
                        prev_len,
                    )
                    .map_err(CliError::TerminalError)?;
                    prev_len = record_map.len();
                }
            }
            // Exit the pipeline
        }

        Err(e) => {
            error!("{}", e);
        }
    }
    Ok(())
}

fn display(
    stdout: &mut Stdout,
    instant: Instant,
    schema: &Schema,
    record_map: &HashMap<Vec<u8>, Vec<Field>>,
    updates_map: &HashMap<Vec<u8>, (i32, Duration)>,
    prev_len: usize,
) -> Result<(), crossterm::ErrorKind> {
    let mut table = Table::new();

    // Fields Row

    let mut cells = vec![];
    for f in &schema.fields {
        cells.push(Cell::new(&f.name));
    }
    table.add_row(Row::new(cells));

    for (key, values) in record_map {
        let mut cells = vec![];
        for v in values {
            let val_str = v.to_string().map_or("".to_string(), |v| v);
            let mut c = Cell::new(&val_str);

            let upd = updates_map.get(key);

            let co = match upd {
                Some((idx, dur)) => {
                    if (instant.elapsed() - *dur) < Duration::from_millis(1000) {
                        if *idx == 0 {
                            color::BRIGHT_RED
                        } else if *idx == 1 {
                            color::GREEN
                        } else {
                            color::YELLOW
                        }
                    } else {
                        color::WHITE
                    }
                }
                None => color::WHITE,
            };

            c.style(dozer_types::prettytable::Attr::ForegroundColor(co));
            cells.push(c);
        }
        table.add_row(Row::new(cells));
    }
    stdout.execute(cursor::RestorePosition)?;

    table.print(stdout.by_ref())?;
    table.printstd();
    stdout.execute(cursor::MoveUp(1))?;

    let diff = prev_len as i64 - record_map.len() as i64;
    if diff > 0 {
        for _i in 0..diff * 3 {
            stdout.execute(cursor::MoveDown(1))?;
            stdout.execute(terminal::Clear(terminal::ClearType::CurrentLine))?;
        }
    }
    stdout.execute(cursor::MoveDown(1))?;
    Ok(())
}
