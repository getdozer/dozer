use std::{collections::HashMap, time::Instant};

use dozer_core::{
    epoch::Epoch,
    event::EventHub,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    chrono::Local,
    errors::internal::BoxedError,
    log::{info, warn},
    node::OpIdentifier,
    types::{FieldType, Operation, Schema, TableOperation},
};
use dozer_types::{log::debug, node::SourceState};

use crate::async_trait::async_trait;

#[derive(Debug)]
pub struct DummySinkFactory;

#[async_trait]
impl SinkFactory for DummySinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_input_port_name(&self, _port: &PortHandle) -> String {
        "dummy".to_string()
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        let inserted_at_index = input_schemas
            .into_values()
            .next()
            .and_then(|schema| {
                schema.fields.into_iter().enumerate().find(|(_, field)| {
                    field.name.to_lowercase() == "inserted_at" && field.typ == FieldType::Timestamp
                })
            })
            .map(|(index, _)| index);
        Ok(Box::new(DummySink {
            inserted_at_index,
            previous_started: Instant::now(),
            count: 0,
            snapshotting_started_instant: HashMap::new(),
            stop_after: std::env::var("STOP_AFTER").map_or(None, |s| s.parse().ok()),
            first_received: None,
            total_latency: 0,
            previous_op_count: 0,
        }))
    }

    fn type_name(&self) -> String {
        "dummy".to_string()
    }
}

#[derive(Debug)]
struct DummySink {
    snapshotting_started_instant: HashMap<String, Instant>,
    inserted_at_index: Option<usize>,
    count: usize,
    previous_started: Instant,
    previous_op_count: usize,
    first_received: Option<Instant>,
    stop_after: Option<i64>,
    total_latency: u64,
}

impl Sink for DummySink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        if self.count == 0 {
            self.first_received = Some(Instant::now());
        }
        let diff = self.count - self.previous_op_count;
        if diff > 1000 {
            if self.count > 0 {
                info!(
                    "Rate: {:.0} op/s, Processed {} records. Elapsed {:?}",
                    diff as f64 / self.previous_started.elapsed().as_secs_f64(),
                    self.count,
                    self.previous_started.elapsed(),
                );
            }
            self.previous_started = Instant::now();
            self.previous_op_count = self.count;
        }

        self.count += match op.op {
            Operation::BatchInsert { ref new } => new.len(),
            _ => 1,
        };

        if let Some(stop_after) = self.stop_after {
            if self.count >= stop_after as usize {
                if let Some(first_received) = self.first_received {
                    info!("Stopping after {} records", stop_after);

                    info!(
                        "Rate: {:.0} op/s, Processed {} records. Elapsed {:?}",
                        stop_after as f64 / first_received.elapsed().as_secs_f64(),
                        self.count,
                        first_received.elapsed(),
                    );

                    if self.total_latency > 0 {
                        info!(
                            "Average latency: {}ms",
                            self.total_latency / stop_after as u64
                        );
                    }
                    std::process::exit(0);
                }
            }
        }

        if let Some(inserted_at_index) = self.inserted_at_index {
            let records = match op.op {
                Operation::BatchInsert { ref new } => new,
                Operation::Insert { ref new } => std::slice::from_ref(new),
                _ => &[],
            };

            for new in records {
                debug!("Received record: {:?}", new);
                let value = &new.values[inserted_at_index];
                if let Some(inserted_at) = value.to_timestamp() {
                    let latency = Local::now().naive_utc() - inserted_at.naive_utc();
                    self.total_latency += latency.num_milliseconds() as u64;
                    info!("Latency: {}ms", latency.num_milliseconds());
                } else {
                    warn!("expecting timestamp, got {:?}", value);
                }
            }
        }
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        connection_name: String,
    ) -> Result<(), BoxedError> {
        self.snapshotting_started_instant
            .insert(connection_name, Instant::now());
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        connection_name: String,
        _id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        if let Some(started_instant) = self.snapshotting_started_instant.remove(&connection_name) {
            info!(
                "Snapshotting for connection {} took {:?}",
                connection_name,
                started_instant.elapsed()
            );
        } else {
            warn!(
                "Snapshotting for connection {} took unknown time",
                connection_name
            );
        }
        Ok(())
    }

    fn set_source_state_data(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state_data(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_source_state(&mut self) -> Result<SourceState, BoxedError> {
        Ok(SourceState::NotStarted)
    }
}
