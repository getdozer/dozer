use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::DEFAULT_PORT_HANDLE;
use crate::dag::node::{PortHandle, Source, SourceFactory};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::thread;
use std::time::Duration;

pub(crate) const GENERATOR_SOURCE_OUTPUT_PORT: PortHandle = DEFAULT_PORT_HANDLE;

pub(crate) struct GeneratorSourceFactory {
    count: u64,
}

impl GeneratorSourceFactory {
    pub fn new(count: u64) -> Self {
        Self { count }
    }
}

impl SourceFactory for GeneratorSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![GENERATOR_SOURCE_OUTPUT_PORT]
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(GeneratorSource { count: self.count })
    }
}

pub(crate) struct GeneratorSource {
    count: u64,
}

impl Source for GeneratorSource {
    fn get_output_schema(&self, port: PortHandle) -> Option<Schema> {
        Some(
            Schema::empty()
                .field(
                    FieldDefinition::new("id".to_string(), FieldType::String, false),
                    true,
                    true,
                )
                .field(
                    FieldDefinition::new("value".to_string(), FieldType::String, false),
                    true,
                    false,
                )
                .clone(),
        )
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..self.count {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{}", n)),
                            Field::String(format!("value_{}", n)),
                        ],
                    ),
                },
                GENERATOR_SOURCE_OUTPUT_PORT,
            )?;
            //  thread::sleep(Duration::from_millis(2));
        }
        fw.terminate()?;
        Ok(())
    }
}
