use dozer_api::async_trait::async_trait;
use std::{collections::HashMap, time::Instant};

use dozer_cache::dozer_log::storage::Queue;
use dozer_core::{
    epoch::Epoch,
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

#[derive(Debug)]
pub struct DummySinkFactory;

#[async_trait]
impl SinkFactory for DummySinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        let inserted_at_index = input_schemas
            .into_values()
            .next()
            .and_then(|schema| {
                schema.fields.into_iter().enumerate().find(|(_, field)| {
                    field.name == "inserted_at" && field.typ == FieldType::Timestamp
                })
            })
            .map(|(index, _)| index);
        Ok(Box::new(DummySink {
            inserted_at_index,
            previous_started: Instant::now(),
            count: 0,
            snapshotting_started_instant: HashMap::new(),
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
}

impl Sink for DummySink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        if self.count % 1000 == 0 {
            if self.count > 0 {
                info!(
                    "Rate: {:.0} op/s, Processed {} records. Elapsed {:?}",
                    1000.0 / self.previous_started.elapsed().as_secs_f64(),
                    self.count,
                    self.previous_started.elapsed(),
                );
            }
            self.previous_started = Instant::now();
        }

        self.count += 1;
        if let Some(inserted_at_index) = self.inserted_at_index {
            if let Operation::Insert { new } = op.op {
                info!("Received record: {:?}", new);
                let value = &new.values[inserted_at_index];
                if let Some(inserted_at) = value.to_timestamp() {
                    let latency = Local::now().naive_utc() - inserted_at.naive_utc();
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

    fn persist(&mut self, _epoch: &Epoch, _queue: &Queue) -> Result<(), BoxedError> {
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

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}
