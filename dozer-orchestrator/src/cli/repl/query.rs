use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cli::init_dozer;
use crate::errors::OrchestrationError;
use crate::Orchestrator;
use dozer_cache::cache::index::get_primary_key;
use dozer_types::crossbeam::channel;
use dozer_types::log::error;
use dozer_types::prettytable::{Cell, Row, Table};
use dozer_types::types::{Field, Operation};

pub fn query(sql: String, config_path: &String) -> Result<(), OrchestrationError> {
    let dozer = init_dozer(config_path.to_owned())?;
    let (sender, receiver) = channel::unbounded::<Operation>();
    let running = Arc::new(AtomicBool::new(true));
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
