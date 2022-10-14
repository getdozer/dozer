use crate::dag::dag::PortHandle;
use crate::dag::forwarder::ProcessorChannelForwarder;
use crate::dag::mt_executor::DEFAULT_PORT_HANDLE;
use crate::dag::node::{Processor, ProcessorFactory};
use crate::state::{StateStore, StateStoreOptions};
use anyhow::{anyhow, Context};
use dozer_types::types::{FieldDefinition, FieldType, Operation, Schema};
use std::collections::HashMap;

pub struct NestedJoinProcessorFactory {
    parent_arr_field: String,
    parent_fk_fields: Vec<String>,
    child_fk_fields: Vec<String>,
}

impl NestedJoinProcessorFactory {
    pub const INPUT_PARENT_PORT_HANDLE: PortHandle = 0_u16;
    pub const INPUT_CHILD_PORT_HANDLE: PortHandle = 1_u16;
    pub const OUTPUT_JOINED_PORT_HANDLE: PortHandle = DEFAULT_PORT_HANDLE;
}

impl ProcessorFactory for NestedJoinProcessorFactory {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        Some(StateStoreOptions {
            allow_duplicate_keys: true,
        })
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![
            NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE,
            NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE,
        ]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(NestedJoinProcessor::new(
            self.parent_arr_field.clone(),
            self.parent_fk_fields.clone(),
            self.child_fk_fields.clone(),
        ))
    }
}

pub struct NestedJoinProcessor {
    parent_arr_field: String,
    parent_fk_fields: Vec<String>,
    child_fk_fields: Vec<String>,
}

impl NestedJoinProcessor {
    pub fn new(
        parent_arr_field: String,
        parent_fk_fields: Vec<String>,
        child_fk_fields: Vec<String>,
    ) -> Self {
        Self {
            parent_arr_field,
            parent_fk_fields,
            child_fk_fields,
        }
    }
}

impl Processor for NestedJoinProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        let parent_schema = input_schemas
            .get(&NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE)
            .context(anyhow!("Unable to find parent schema"))?;
        let child_schema = input_schemas
            .get(&NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE)
            .context(anyhow!("Unable to find child schema"))?;

        let mut out_schema = Schema::empty();
        let mut nested_found = false;
        for f in &parent_schema.fields {
            if f.name == self.parent_arr_field {
                out_schema.fields.push(FieldDefinition::new(
                    f.name.clone(),
                    FieldType::RecordArray(child_schema.clone()),
                    false,
                ));
                nested_found = true;
            } else {
                out_schema.fields.push(f.clone());
            }
        }

        if !nested_found {
            return Err(anyhow!(
                "Unable to find field {} in the parent schema",
                self.parent_arr_field
            ));
        }

        out_schema.values = parent_schema.values.clone();
        out_schema.primary_index = parent_schema.primary_index.clone();
        Ok(out_schema)
    }

    fn init(&mut self, state: &mut dyn StateStore) -> anyhow::Result<()> {
        todo!()
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        state: &mut dyn StateStore,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
