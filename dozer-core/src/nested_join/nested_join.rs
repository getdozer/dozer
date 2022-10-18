use crate::dag::dag::PortHandle;
use crate::dag::forwarder::ProcessorChannelForwarder;
use crate::dag::mt_executor::DEFAULT_PORT_HANDLE;
use crate::dag::node::{Processor, ProcessorFactory};
use crate::state::{StateStore, StateStoreOptions};
use anyhow::{anyhow, Context};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use log::{error, warn};
use std::collections::HashMap;

pub struct NestedJoinProcessorFactory {
    parent_array_field: String,
    parent_join_key_fields: Vec<String>,
    child_join_key_fields: Vec<String>,
}

const PARENT_PRIMARY_KEY_INDEX_ID: u16 = NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE;
const PARENT_JOIN_KEY_INDEX_ID: u16 = NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE + 1;
const CHILD_PRIMARY_KEY_INDEX_ID: u16 = NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE;
const CHILD_JOIN_KEY_INDEX_ID: u16 = NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE + 1;

enum NestedOperation {
    Insert,
    Delete,
}

impl NestedJoinProcessorFactory {
    pub const INPUT_PARENT_PORT_HANDLE: PortHandle = 10_u16;
    pub const INPUT_CHILD_PORT_HANDLE: PortHandle = 20_u16;
    pub const OUTPUT_JOINED_PORT_HANDLE: PortHandle = DEFAULT_PORT_HANDLE;
    pub fn new(
        parent_array_field: String,
        parent_join_key_fields: Vec<String>,
        child_join_key_fields: Vec<String>,
    ) -> Self {
        Self {
            parent_array_field,
            parent_join_key_fields,
            child_join_key_fields,
        }
    }
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
            self.parent_array_field.clone(),
            self.parent_join_key_fields.clone(),
            self.child_join_key_fields.clone(),
        ))
    }
}

struct Indexes {
    parent_schema: Schema,
    child_schema: Schema,
    parent_join_key_indexes: Vec<usize>,
    child_join_key_indexes: Vec<usize>,
    parent_array_index: usize,
}

pub struct NestedJoinProcessor {
    parent_array_field: String,
    parent_join_key_fields: Vec<String>,
    child_join_key_fields: Vec<String>,
    indexes: Option<Indexes>,
}

impl NestedJoinProcessor {
    fn process_child_op(
        &self,
        op: Operation,
        idx: &Indexes,
        state: &mut dyn StateStore,
    ) -> anyhow::Result<Vec<Operation>> {
        match op {
            Operation::Insert { new } => {
                if idx.child_schema.primary_index.is_empty() {
                } else {
                    let primary_key = new.get_key(
                        &idx.child_schema.primary_index,
                        Some(CHILD_PRIMARY_KEY_INDEX_ID.to_ne_bytes().as_slice()),
                    )?;
                    let val = bincode::serialize(&new).unwrap();
                    state.put(primary_key.as_slice(), val.as_slice())?;

                    let join_key = new.get_key(
                        &idx.child_join_key_indexes,
                        Some(CHILD_JOIN_KEY_INDEX_ID.to_ne_bytes().as_slice()),
                    )?;
                    state.put(join_key.as_slice(), primary_key.as_slice())?;

                    let lookup_key = new.get_key(
                        &idx.child_join_key_indexes,
                        Some(PARENT_JOIN_KEY_INDEX_ID.to_ne_bytes().as_slice()),
                    )?;

                    if let Some(pk) = state.get(lookup_key.as_slice())? {
                        // Lookup parent
                    }
                }
                Ok(vec![])
            }
            Operation::Update { old, new } => Ok(vec![]),
            Operation::Delete { old } => Ok(vec![]),
        }
    }

    fn get_all_children(
        &self,
        key: Vec<u8>,
        state: &mut dyn StateStore,
        op: NestedOperation,
    ) -> anyhow::Result<Vec<Operation>> {
        let mut records = Vec::<Operation>::new();

        let mut cursor = state.cursor()?;
        if cursor.seek(key.as_slice())? {
            loop {
                let kv = cursor.read()?;
                match kv {
                    Some(t) => {
                        if t.0 != key.as_slice() {
                            break;
                        }
                        let record = state.get(t.1)?;
                        match record {
                            Some(v) => {
                                let rec: Record = bincode::deserialize(v)?;
                                match op {
                                    NestedOperation::Insert => records.push(Operation::Insert {new: rec}),
                                    NestedOperation::Delete => records.push(Operation::Delete {old: rec})
                                }
                            }
                            _ => error!(
                                "Found reference to primary key {:x?}, but primary key does not exist",
                                t.1
                            ),
                        }
                    }
                    _ => break,
                }
                if !cursor.next()? {
                    break;
                }
            }
        }
        Ok(records)
    }

    fn process_parent_op(
        &self,
        op: Operation,
        idx: &Indexes,
        state: &mut dyn StateStore,
    ) -> anyhow::Result<Vec<Operation>> {
        match op {
            Operation::Update { old, new } => Ok(vec![]),
            Operation::Insert { mut new } => {
                if idx.parent_schema.primary_index.is_empty() {
                } else {
                    let primary_key = new.get_key(
                        &idx.parent_schema.primary_index,
                        Some(PARENT_PRIMARY_KEY_INDEX_ID.to_ne_bytes().as_slice()),
                    )?;
                    let val = bincode::serialize(&new).unwrap();
                    state.put(primary_key.as_slice(), val.as_slice())?;

                    let join_key = new.get_key(
                        &idx.parent_join_key_indexes,
                        Some(PARENT_JOIN_KEY_INDEX_ID.to_ne_bytes().as_slice()),
                    )?;
                    state.put(join_key.as_slice(), primary_key.as_slice())?;

                    let lookup_key = new.get_key(
                        &idx.parent_join_key_indexes,
                        Some(CHILD_JOIN_KEY_INDEX_ID.to_ne_bytes().as_slice()),
                    )?;
                    let children =
                        self.get_all_children(lookup_key, state, NestedOperation::Insert)?;
                    new.values[idx.parent_array_index] = Field::RecordArray(children);
                }

                Ok(vec![Operation::Insert { new }])
            }
            Operation::Delete { old } => Ok(vec![]),
        }
    }

    pub(crate) fn process_op(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        state: &mut dyn StateStore,
    ) -> anyhow::Result<Vec<Operation>> {
        let indexes = self
            .indexes
            .as_ref()
            .context(anyhow!("Schema must be defined"))?;

        match from_port {
            NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE => {
                self.process_parent_op(op, indexes, state)
            }
            _ => self.process_child_op(op, indexes, state),
        }
    }

    pub(crate) fn update_schema_op(
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
            if f.name == self.parent_array_field {
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
                self.parent_array_field
            ));
        }

        let mut parent_fk_indexes = Vec::<usize>::new();
        for name in &self.parent_join_key_fields {
            parent_fk_indexes.push(parent_schema.get_field_index(name.as_str())?.0);
        }

        let mut child_fk_indexes = Vec::<usize>::new();
        for name in &self.child_join_key_fields {
            child_fk_indexes.push(child_schema.get_field_index(name.as_str())?.0);
        }

        let idx = Indexes {
            parent_join_key_indexes: parent_fk_indexes,
            child_join_key_indexes: child_fk_indexes,
            parent_array_index: parent_schema
                .get_field_index(self.parent_array_field.as_str())?
                .0,
            parent_schema: parent_schema.clone(),
            child_schema: child_schema.clone(),
        };

        self.indexes = Some(idx);

        out_schema.values = parent_schema.values.clone();
        out_schema.primary_index = parent_schema.primary_index.clone();
        Ok(out_schema)
    }

    pub fn new(
        parent_array_field: String,
        parent_join_key_fields: Vec<String>,
        child_join_key_fields: Vec<String>,
    ) -> Self {
        Self {
            parent_array_field,
            parent_join_key_fields,
            child_join_key_fields,
            indexes: None,
        }
    }
}

impl Processor for NestedJoinProcessor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        self.update_schema_op(output_port, input_schemas)
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
        let ret_ops = self.process_op(from_port, op, state)?;
        for op in ret_ops {
            fw.send(op, NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE)?;
        }
        Ok(())
    }
}
