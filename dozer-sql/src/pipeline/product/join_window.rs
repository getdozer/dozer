use std::collections::HashMap;

use dozer_core::{node::PortHandle, record_store::RecordReader};
use dozer_types::types::{Record, Schema};

use crate::pipeline::errors::JoinError;

use super::{
    join::JoinAction,
    join_index::{decode_lookup_key, encode_lookup_key, get_primary_key_fields},
    join_table::JoinTable,
    window::WindowType,
};

#[derive(Clone, Debug)]
pub struct JoinWindow {
    port: PortHandle,

    pub schema: Schema,

    window: WindowType,

    table: JoinTable,
}

impl JoinWindow {
    pub fn new(port: PortHandle, schema: Schema, window: WindowType, table: JoinTable) -> Self {
        Self {
            port,
            schema,
            window,
            table,
        }
    }

    pub fn get_source(&self) -> PortHandle {
        self.port
    }

    pub fn execute(
        &self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, JoinError> {
        debug_assert!(self.port == from_port);
        let source_outputs = self.table.execute(action, from_port, record)?;

        let mut window_outputs = vec![];
        for (action, record, lookup_key) in source_outputs.into_iter() {
            let window_records = self
                .window
                .execute(&record)
                .map_err(|_| JoinError::InvalidSource(0))?;
            for window_record in window_records {
                // let lookup_key = self.table.encode_lookup_key(&window_record, &self.schema)?;
                window_outputs.push((action.clone(), window_record, lookup_key.clone()));
            }
        }
        Ok(window_outputs)
    }

    pub fn lookup(
        &self,
        lookup_key: &[u8],
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, JoinError> {
        // split lookup key into fields
        let (version, mut fields) =
            decode_lookup_key(lookup_key).map_err(JoinError::DeserializationError)?;

        // Assuming the Window operator is always appending only 1 field to the primary key
        fields.pop();

        // create a new lookup key using only the source key fields
        let lookup_key = encode_lookup_key(version, &fields);

        // execute the lookup using only the source key fields
        let source_outputs = self.table.lookup(&lookup_key, readers)?;

        // for each record, execute the window
        let mut window_outputs = vec![];
        for (record, _) in source_outputs.into_iter() {
            let window_records = self
                .window
                .execute(&record)
                .map_err(|_| JoinError::InvalidSource(0))?;
            for window_record in window_records {
                let pk_fields = get_primary_key_fields(&window_record, &self.schema)?;
                let window_lookup_key = encode_lookup_key(window_record.version, &pk_fields);
                if window_lookup_key == lookup_key {
                    window_outputs.push((window_record, lookup_key.clone()));
                }
            }
        }
        Ok(window_outputs)
    }
}
