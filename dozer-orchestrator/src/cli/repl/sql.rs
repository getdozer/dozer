use crate::errors::CliError;
use crate::utils::get_sql_history_path;
use std::collections::HashMap;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cli::{init_dozer, load_config};
use crate::errors::OrchestrationError;
use crate::Orchestrator;
use crossterm::cursor;
use dozer_cache::cache::index::get_primary_key;
use dozer_types::crossbeam::channel;
use dozer_types::log::{debug, error, info};
use dozer_types::prettytable::color;
use dozer_types::prettytable::{Cell, Row, Table};
use dozer_types::types::{Field, Operation};
use rustyline::error::ReadlineError;

pub fn editor(config_path: &String) -> Result<(), OrchestrationError> {
    let mut rl = rustyline::Editor::<()>::new()
        .map_err(|e| OrchestrationError::CliError(CliError::ReadlineError(e)))?;

    let config = load_config(config_path.clone())?;

    let history_path = get_sql_history_path(&config);

    if rl.load_history(history_path.as_path()).is_err() {
        debug!("No previous history file found.");
    }
    loop {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if !line.is_empty() {
                    query(line, &config_path)?;
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

pub fn query(sql: String, config_path: &String) -> Result<(), OrchestrationError> {
    let dozer = init_dozer(config_path.to_owned())?;
    let (sender, receiver) = channel::unbounded::<Operation>();
    let running = Arc::new(AtomicBool::new(true));
    let res = dozer.query(sql, sender, running.clone());
    let cursor = cursor();
    cursor.save_position().unwrap();
    match res {
        Ok(schema) => {
            let mut record_map: HashMap<Vec<u8>, Vec<Field>> = HashMap::new();
            let mut idx: u64 = 0;
            // Exit after 10 records
            let _expected: usize = 10;
            // Wait for 500 * 100 timeout
            let mut times = 0;
            let instant = Instant::now();
            let mut last_shown = Duration::from_millis(0);
            let mut last_len = 0;

            let display =
                |record_map: &HashMap<Vec<u8>, Vec<Field>>,
                 updates_map: &HashMap<Vec<u8>, (i32, Duration)>| {
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
                                    if (instant.elapsed() - dur.clone())
                                        < Duration::from_millis(1000)
                                    {
                                        if *idx == 0 {
                                            color::BRIGHT_RED
                                        } else {
                                            color::GREEN
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
                    cursor.restore_position().unwrap();
                    table.printstd();
                };

            let pkey_index = if schema.primary_index.is_empty() {
                vec![0]
            } else {
                schema.primary_index
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
                                record_map.remove(&pkey);
                                updates_map.insert(pkey.clone(), (0, instant.elapsed()));

                                let pkey = if pkey_index.is_empty() {
                                    idx.to_le_bytes().to_vec()
                                } else {
                                    get_primary_key(&pkey_index, &new.values)
                                };
                                record_map.insert(pkey.clone(), new.values);
                                updates_map.insert(pkey, (1, instant.elapsed()));
                            }
                        }
                        // display(&record_map, &updates_map);
                        idx += 1;
                    }
                    Err(e) => match e {
                        channel::RecvTimeoutError::Timeout => {
                            times += 1;
                        }
                        channel::RecvTimeoutError::Disconnected => {
                            // error!("channel disconnected");
                            // break;
                        }
                    },
                }

                if instant.elapsed() - last_shown > Duration::from_millis(100) {
                    last_shown = instant.elapsed();
                    display(&record_map, &updates_map);
                }
            }
            // Exit the pipeline
            display(&record_map, &updates_map);
            running.store(false, Ordering::Relaxed);
        }

        Err(e) => {
            error!("{}", e);
        }
    }

    Ok(())
}
