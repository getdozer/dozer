use std::collections::HashMap;

use dozer_core::{
    dag::{errors::ExecutionError, node::PortHandle, record_store::RecordReader},
    storage::{
        errors::StorageError, lmdb_storage::SharedTransaction,
        prefix_transaction::PrefixTransaction,
    },
};
use dozer_types::{
    errors::types::TypeError,
    types::{Record, Schema},
};
use lmdb::Database;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinAction {
    Insert,
    Delete,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOperatorType {
    Inner,
    LeftOuter,
    RightOuter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinConstraint {
    pub left_key_index: usize,
    pub right_key_index: usize,
}

#[derive(Clone, Debug)]
pub enum JoinSource {
    Table(JoinTable),
    Join(JoinOperator),
}

impl JoinSource {
    pub fn insert(
        &self,
        action: &JoinAction,
        from_port: PortHandle,
        record: &Record,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ExecutionError> {
        match self {
            JoinSource::Table(table) => table.insert(from_port, record),
            JoinSource::Join(join) => {
                join.insert(action, from_port, record, database, transaction, readers)
            }
        }
    }

    pub fn lookup(
        &self,
        lookup_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ExecutionError> {
        match self {
            JoinSource::Table(table) => table.lookup(lookup_key, readers),
            JoinSource::Join(join) => join.lookup(lookup_key, database, transaction, readers),
        }
    }

    // pub fn encode_lookup_key(
    //     &self,
    //     record: &Record,
    //     schema: &Schema,
    // ) -> Result<Vec<u8>, TypeError> {
    //     match self {
    //         JoinSource::Table(table) => table.encode_lookup_key(record, &table.schema),
    //         JoinSource::Join(join) => join.encode_lookup_key(record, schema),
    //     }
    // }

    // fn decode_lookup_key(&self, lookup_key: &[u8]) -> (u32, Vec<u8>) {
    //     match self {
    //         JoinSource::Table(table) => table.decode_lookup_key(record, &table.schema),
    //         JoinSource::Join(join) => join.encode_lookup_key(record, schema),
    //     }
    // }

    pub fn get_output_schema(&self) -> Schema {
        match self {
            JoinSource::Table(table) => table.schema.clone(),
            JoinSource::Join(join) => join.schema.clone(),
        }
    }

    pub fn get_sources(&self) -> Vec<PortHandle> {
        match self {
            JoinSource::Table(table) => vec![table.get_source()],
            JoinSource::Join(join) => join.get_sources(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JoinTable {
    port: PortHandle,

    pub schema: Schema,
}

impl JoinTable {
    pub fn new(port: PortHandle, schema: Schema) -> Self {
        Self { port, schema }
    }

    pub fn get_source(&self) -> PortHandle {
        self.port
    }

    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(Record, Vec<u8>)>, ExecutionError> {
        if self.port == from_port {
            let lookup_key = self.encode_lookup_key(record, &self.schema)?;
            Ok(vec![(record.clone(), lookup_key)])
        } else {
            Err(ExecutionError::InvalidPortHandle(self.port))
        }
    }

    fn lookup(
        &self,
        lookup_key: &[u8],
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ExecutionError> {
        let reader = readers
            .get(&self.port)
            .ok_or(ExecutionError::InvalidPortHandle(self.port))?;

        let (version, id) = self.decode_lookup_key(lookup_key);

        let mut output_records = vec![];
        if let Some(record) = reader.get(&id, version)? {
            output_records.push((record, lookup_key.to_vec()));
        }
        Ok(output_records)
    }

    fn encode_lookup_key(&self, record: &Record, schema: &Schema) -> Result<Vec<u8>, TypeError> {
        let mut lookup_key = Vec::with_capacity(64);
        if let Some(version) = record.version {
            lookup_key.extend_from_slice(&version.to_be_bytes());
        } else {
            lookup_key.extend_from_slice(&[0_u8; 4]);
        }

        for key_index in schema.primary_index.iter() {
            let key_value = record.get_value(*key_index)?;
            let key_bytes = key_value.encode();
            lookup_key.extend_from_slice(&key_bytes);
        }

        Ok(lookup_key)
    }

    fn decode_lookup_key(&self, lookup_key: &[u8]) -> (u32, Vec<u8>) {
        let (version_bytes, id) = lookup_key.split_at(4);
        let version = u32::from_be_bytes(version_bytes.try_into().unwrap());
        (version, id.to_vec())
    }
}

#[derive(Clone, Debug)]
pub struct JoinOperator {
    _operator: JoinOperatorType,

    left_join_key: Vec<usize>,
    right_join_key: Vec<usize>,

    schema: Schema,

    left_source: Box<JoinSource>,
    right_source: Box<JoinSource>,

    // Lookup indexes
    left_lookup_index: u32,

    right_lookup_index: u32,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        left_join_key: Vec<usize>,
        right_join_key: Vec<usize>,
        schema: Schema,
        left_source: Box<JoinSource>,
        right_source: Box<JoinSource>,
        left_lookup_index: u32,
        right_lookup_index: u32,
    ) -> Self {
        Self {
            _operator: operator,
            left_join_key,
            right_join_key,
            schema,
            left_source,
            right_source,
            left_lookup_index,
            right_lookup_index,
        }
    }

    pub fn get_sources(&self) -> Vec<PortHandle> {
        [
            self.left_source.get_sources().as_slice(),
            self.right_source.get_sources().as_slice(),
        ]
        .concat()
    }

    pub fn insert(
        &self,
        action: &JoinAction,
        from_port: PortHandle,
        record: &Record,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ExecutionError> {
        // if the source port is under the left branch of the join
        if self.left_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut left_records = self.left_source.insert(
                action,
                from_port,
                record,
                database,
                transaction,
                readers,
            )?;

            let mut right_records = vec![];
            for (left_record, left_lookup_key) in left_records.iter_mut() {
                let join_key: Vec<u8> = encode_join_key(left_record, &self.left_join_key)?;

                // update left join index
                self.update_index(
                    action,
                    &join_key,
                    left_lookup_key,
                    self.left_lookup_index,
                    database,
                    transaction,
                )?;

                let right_lookup_keys =
                    self.read_index(&join_key, self.right_lookup_index, database, transaction)?;

                for right_lookup_key in right_lookup_keys.iter() {
                    // lookup on the right branch to find matching records
                    let mut right_lookup_records = self.right_source.lookup(
                        right_lookup_key,
                        database,
                        transaction,
                        readers,
                    )?;
                    right_records.append(&mut right_lookup_records);
                }
            }

            // join the records
            for (left_record, left_lookup_key) in left_records.iter_mut() {
                let join_key: Vec<u8> = encode_join_key(left_record, &self.left_join_key)?;

                for (right_record, right_lookup_key) in right_records.iter_mut() {
                    // update right join index
                    self.update_index(
                        action,
                        &join_key,
                        right_lookup_key,
                        self.right_lookup_index,
                        database,
                        transaction,
                    )?;

                    let join_record = join_records(left_record, right_record);
                    let join_lookup_key =
                        self.encode_join_lookup_key(left_lookup_key, right_lookup_key);

                    output_records.push((join_record, join_lookup_key));
                }
            }

            Ok(output_records)
        } else if self.right_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut right_records = self.right_source.insert(
                action,
                from_port,
                record,
                database,
                transaction,
                readers,
            )?;

            let mut left_records = vec![];

            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let join_key: Vec<u8> = encode_join_key(right_record, &self.right_join_key)?;

                // update right join index
                self.update_index(
                    action,
                    &join_key,
                    right_lookup_key,
                    self.right_lookup_index,
                    database,
                    transaction,
                )?;

                let left_lookup_keys =
                    self.read_index(&join_key, self.left_lookup_index, database, transaction)?;

                for left_lookup_key in left_lookup_keys.iter() {
                    // lookup on the left branch to find matching records
                    let mut left_lookup_records =
                        self.left_source
                            .lookup(left_lookup_key, database, transaction, readers)?;
                    left_records.append(&mut left_lookup_records);
                }
            }

            // join the records
            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let join_key: Vec<u8> = encode_join_key(right_record, &self.right_join_key)?;

                for (left_record, left_lookup_key) in left_records.iter_mut() {
                    // update left join index
                    self.update_index(
                        action,
                        &join_key,
                        left_lookup_key,
                        self.left_lookup_index,
                        database,
                        transaction,
                    )?;

                    let join_record = join_records(left_record, right_record);
                    let join_lookup_key =
                        self.encode_join_lookup_key(left_lookup_key, right_lookup_key);
                    output_records.push((join_record, join_lookup_key));
                }
            }

            return Ok(output_records);
        } else {
            return Err(ExecutionError::InvalidPortHandle(from_port));
        }
    }

    fn lookup(
        &self,
        lookup_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ExecutionError> {
        let mut output_records = vec![];

        let (left_loookup_key, right_lookup_key) = self.decode_join_lookup_key(lookup_key);

        let mut left_records =
            self.left_source
                .lookup(&left_loookup_key, database, transaction, readers)?;

        let mut right_records =
            self.right_source
                .lookup(&right_lookup_key, database, transaction, readers)?;

        for (left_record, left_lookup_key) in left_records.iter_mut() {
            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);

                output_records.push((join_record, join_lookup_key));
            }
        }

        Ok(output_records)
    }

    pub fn update_index(
        &self,
        action: &JoinAction,
        key: &[u8],
        value: &[u8],
        prefix: u32,
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);

        match *action {
            JoinAction::Insert => {
                prefix_transaction.put(*database, key, value)?;
            }
            JoinAction::Delete => {
                prefix_transaction.del(*database, key, Some(value))?;
            }
            JoinAction::Update => {
                todo!()
            }
        }

        Ok(())
    }

    // fn encode_lookup_key(record: &Record, schema: &Schema) -> Result<Vec<u8>, TypeError> {
    //     let mut lookup_key = Vec::with_capacity(64);
    //     if let Some(version) = record.version {
    //         lookup_key.extend_from_slice(&version.to_be_bytes());
    //     } else {
    //         lookup_key.extend_from_slice(&[0_u8; 4]);
    //     }

    //     let key = get_composite_key(record, schema.primary_index.as_slice())?;
    //     lookup_key.extend(key);
    //     Ok(lookup_key)
    // }

    fn read_index(
        &self,
        join_key: &[u8],
        prefix: u32,
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let mut join_keys = vec![];

        let mut exclusive_transaction = transaction.write();
        let right_prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);

        let cursor = right_prefix_transaction.open_cursor(*database)?;

        if !cursor.seek(join_key)? {
            return Ok(join_keys);
        }

        loop {
            let entry = cursor.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;

            if entry.0 != join_key {
                break;
            }

            join_keys.push(entry.1.to_vec());

            if !cursor.next()? {
                break;
            }
        }

        Ok(join_keys)
    }

    fn encode_join_lookup_key(&self, left_lookup_key: &[u8], right_lookup_key: &[u8]) -> Vec<u8> {
        let mut composite_lookup_key = Vec::with_capacity(64);
        composite_lookup_key.extend_from_slice(&(left_lookup_key.len() as u32).to_be_bytes());
        composite_lookup_key.extend_from_slice(left_lookup_key);
        composite_lookup_key.extend_from_slice(&(right_lookup_key.len() as u32).to_be_bytes());
        composite_lookup_key.extend_from_slice(right_lookup_key);
        composite_lookup_key
    }

    fn decode_join_lookup_key(&self, join_lookup_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut offset = 0;

        let left_length = u32::from_be_bytes([
            join_lookup_key[offset],
            join_lookup_key[offset + 1],
            join_lookup_key[offset + 2],
            join_lookup_key[offset + 3],
        ]);
        offset += 4;
        let left_lookup_key = &join_lookup_key[offset..offset + left_length as usize];
        offset += left_length as usize;

        let right_length = u32::from_be_bytes([
            join_lookup_key[offset],
            join_lookup_key[offset + 1],
            join_lookup_key[offset + 2],
            join_lookup_key[offset + 3],
        ]);
        offset += 4;
        let right_lookup_key = &join_lookup_key[offset..offset + right_length as usize];

        (left_lookup_key.to_vec(), right_lookup_key.to_vec())
    }
}

// fn encode_composite_lookup_key(left_join_keys: Vec<Field>) -> Vec<u8> {
//     let mut composite_lookup_key = vec![];
//     for key in left_join_keys.iter() {
//         let value = key.encode();
//         let length = value.len() as u32;
//         composite_lookup_key.extend_from_slice(&length.to_be_bytes());
//         composite_lookup_key.extend_from_slice(value.as_slice());
//     }
//     composite_lookup_key
// }

// fn encode_composite_lookup_key_from_record(
//     record: &Record,
//     merged_join_keys: Vec<usize>,
// ) -> Vec<u8> {
//     let mut composite_lookup_key = vec![];
//     for key in merged_join_keys.iter() {
//         let value = &record.values[*key].encode();
//         let length = value.len() as u32;
//         composite_lookup_key.extend_from_slice(&length.to_be_bytes());
//         composite_lookup_key.extend_from_slice(value.as_slice());
//     }
//     composite_lookup_key
// }

// fn decode_composite_lookup_key(
//     binary_lookup_key: Vec<u8>,
//     merged_join_keys: Vec<usize>,
// ) -> Result<Vec<Field>, DeserializationError> {
//     let mut lookup_key = vec![];
//     let mut offset = 0;
//     for _index in merged_join_keys.iter() {
//         let length = u32::from_be_bytes([
//             binary_lookup_key[offset],
//             binary_lookup_key[offset + 1],
//             binary_lookup_key[offset + 2],
//             binary_lookup_key[offset + 3],
//         ]);
//         offset += 4;
//         let value = &binary_lookup_key[offset..offset + length as usize];
//         offset += length as usize;
//         lookup_key.push(Field::decode(value)?);
//     }
//     Ok(lookup_key)
// }

// fn merge_join_keys(
//     left_join_key: &[usize],
//     left_schema_len: &usize,
//     right_join_key: &[usize],
// ) -> Vec<usize> {
//     let mut merged_join_keys = left_join_key.to_vec();
//     for key in right_join_key.iter() {
//         merged_join_keys.push(key + left_schema_len);
//     }
//     merged_join_keys
// }

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
    Record::new(None, concat_values, None)
}

fn encode_join_key(record: &Record, join_keys: &[usize]) -> Result<Vec<u8>, TypeError> {
    let mut composite_lookup_key = vec![];
    for key in join_keys.iter() {
        let value = &record.values[*key].encode();
        let length = value.len() as u32;
        composite_lookup_key.extend_from_slice(&length.to_be_bytes());
        composite_lookup_key.extend_from_slice(value.as_slice());
    }
    Ok(composite_lookup_key)
}
