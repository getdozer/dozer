use std::collections::HashMap;

use dozer_core::{
    dag::{errors::ExecutionError, node::PortHandle, record_store::RecordReader},
    storage::{
        errors::StorageError, lmdb_storage::SharedTransaction,
        prefix_transaction::PrefixTransaction,
    },
};
use dozer_types::{
    errors::types::{DeserializationError, TypeError},
    types::{Field, Record, Schema},
};
use lmdb::Database;
use sqlparser::keywords::RIGHT;

use crate::pipeline::errors::JoinError;

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
        from_port: PortHandle,
        record: &Record,
        join_keys: &[usize],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        match self {
            JoinSource::Table(table) => {
                table.insert(from_port, record, join_keys, database, transaction, readers)
            }
            JoinSource::Join(join) => {
                join.insert(from_port, record, join_keys, database, transaction, readers)
            }
        }
    }

    pub fn lookup(
        &self,
        from_port: PortHandle,
        record: &Record,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        match self {
            JoinSource::Table(table) => {
                table.lookup(from_port, record, join_key, database, transaction, readers)
            }
            JoinSource::Join(join) => {
                join.lookup(from_port, record, join_key, database, transaction, readers)
            }
        }
    }

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

    join_index: u32,
}

impl JoinTable {
    pub fn new(port: PortHandle, schema: Schema, join_index: u32) -> Self {
        Self {
            port,
            schema,
            join_index,
        }
    }

    pub fn get_source(&self) -> PortHandle {
        self.port
    }

    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        join_keys: &[usize],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let join_key = get_join_key(record, join_keys)?;
        let lookup_key: Vec<u8> = get_lookup_key(record, &self.schema)?;
        self.insert_index(&join_key, &lookup_key, database, transaction)?;

        // if the source port is the same as the port of the incoming message, return the record
        if self.port == from_port {
            Ok(vec![record.clone()])
        } else {
            Err(ExecutionError::InvalidPortHandle(self.port))
        }
    }

    fn lookup(
        &self,
        from_port: PortHandle,
        record: &Record,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        // if the source port is the same as the port of the incoming message, return the record
        if self.port == from_port {
            return Ok(vec![record.clone()]);
        }

        let mut result_records = vec![];

        let reader = readers
            .get(&self.port)
            .ok_or(ExecutionError::InvalidPortHandle(self.port))?;

        // let join_key = get_join_key(record, join_keys)?;
        let lookup_keys = self.get_lookup_keys(&join_key, database, transaction)?;

        // retrieve records for the table on the right side of the join
        for (lookup_key, lookup_version) in lookup_keys.iter() {
            if let Some(left_record) = reader.get(lookup_key, *lookup_version)? {
                let join_record = join_records(&mut left_record.clone(), &mut record.clone());
                result_records.push(join_record);
            }
        }

        Ok(result_records)
    }

    pub fn insert_index(
        &self,
        key: &[u8],
        value: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.join_index);

        prefix_transaction.put(*database, key, value)?;

        Ok(())
    }

    // todo: isolate the index logic and make it generic (see the todo in the JoinOperator get_lookup_keys)
    fn get_lookup_keys(
        &self,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<(Vec<u8>, u32)>, ExecutionError> {
        let mut join_keys = vec![];

        let mut exclusive_transaction = transaction.write();
        let right_prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.join_index);

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

            let (version_bytes, key_bytes) = entry.1.split_at(4);
            let version = u32::from_be_bytes(version_bytes.try_into().unwrap());
            join_keys.push((key_bytes.to_vec(), version));

            if !cursor.next()? {
                break;
            }
        }

        Ok(join_keys)
    }
}

#[derive(Clone, Debug)]
pub struct JoinOperator {
    operator: JoinOperatorType,

    left_join_key: Vec<usize>,
    right_join_key: Vec<usize>,

    schema: Schema,

    left_source: Box<JoinSource>,
    right_source: Box<JoinSource>,

    // Lookup indexes
    left_index: u32,
    right_index: u32,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        left_join_key: Vec<usize>,
        right_join_key: Vec<usize>,
        schema: Schema,
        left_source: Box<JoinSource>,
        right_source: Box<JoinSource>,
        left_index: u32,
        right_index: u32,
    ) -> Self {
        Self {
            operator,
            left_join_key,
            right_join_key,
            schema,
            left_source,
            right_source,
            left_index,
            right_index,
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
        from_port: PortHandle,
        record: &Record,
        join_keys: &[usize],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        // if the source port is under the left branch of the join
        if self.left_source.get_sources().contains(&from_port) {
            // forward the record and the current join constraints to the left source
            let mut left_records = self.left_source.insert(
                from_port,
                record,
                &self.left_join_key,
                database,
                transaction,
                readers,
            )?;

            let join_key: Vec<u8> = get_join_key(record, &self.left_join_key)?;
            // lookup on the right branch to find matching records
            let mut right_records = self.right_source.lookup(
                from_port,
                record,
                &join_key,
                database,
                transaction,
                readers,
            )?;

            // join the records
            let mut output_records = vec![];
            for left_record in left_records.iter_mut() {
                for right_record in right_records.iter_mut() {
                    let join_record = join_records(left_record, right_record);
                    output_records.push(join_record);
                }
            }

            // update the lookup index
            for record in output_records.iter() {
                // get the join key for the merged record, which is the key for the lookup index
                let join_key: Vec<u8> = get_join_key(&record, join_keys)?;
                // generate the composite value using left and right constraints, which is the value for the lookup index
                // in the lookup operation, the composite value will be split back into left and right constraints
                // and forwarded to the left and right sources for lookup
                let merged_join_keys = merge_join_keys(&self.left_join_key, &self.right_join_key);
                let composite_lookup_key: Vec<u8> =
                    encode_composite_lookup_key_from_record(record, merged_join_keys);
                self.insert_index(&join_key, &composite_lookup_key, database, transaction)?;
            }

            return Ok(output_records);
        } else if self.right_source.get_sources().contains(&from_port) {
            let mut right_records = self.right_source.insert(
                from_port,
                record,
                join_keys,
                database,
                transaction,
                readers,
            )?;

            // lookup on the left branch to find matching records
            let join_key: Vec<u8> = get_join_key(record, &self.right_join_key)?;
            let mut left_records = self.left_source.lookup(
                from_port,
                record,
                &join_key,
                database,
                transaction,
                readers,
            )?;

            // join the records
            let mut output_records = vec![];
            for left_record in left_records.iter_mut() {
                for right_record in right_records.iter_mut() {
                    let join_record = join_records(left_record, right_record);
                    output_records.push(join_record);
                }
            }

            // update the lookup index
            for record in output_records.iter() {
                // get the join key for the merged record, which is the key for the lookup index
                let join_key: Vec<u8> = get_join_key(&record, join_keys)?;
                // generate the composite value using left and right constraints, which is the value for the lookup index
                // in the lookup operation, the composite value will be split back into left and right constraints
                // and forwarded to the left and right sources for lookup
                let merged_join_keys = merge_join_keys(&self.left_join_key, &self.right_join_key);
                let composite_lookup_key: Vec<u8> =
                    encode_composite_lookup_key_from_record(record, merged_join_keys);
                self.insert_index(&join_key, &composite_lookup_key, database, transaction)?;
            }

            return Ok(output_records);
        } else {
            return Err(ExecutionError::InvalidPortHandle(from_port));
        }
    }

    fn lookup(
        &self,
        from_port: PortHandle,
        record: &Record,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let mut output_records = vec![];

        let composite_lookup_keys = self.get_lookup_keys(join_key, database, transaction)?;

        for binary_lookup_key in composite_lookup_keys.iter() {
            let merged_join_keys = merge_join_keys(&self.left_join_key, &self.right_join_key);

            if let Ok(join_keys) =
                decode_composite_lookup_key(binary_lookup_key.to_vec(), merged_join_keys)
            {
                let left_join_keys = join_keys[0..self.left_join_key.len()].to_vec();
                let join_key: Vec<u8> = encode_composite_lookup_key(left_join_keys);

                let mut left_records = self.left_source.lookup(
                    from_port,
                    record,
                    &join_key,
                    database,
                    transaction,
                    readers,
                )?;

                let right_join_keys = join_keys[self.left_join_key.len()..].to_vec();
                let join_key: Vec<u8> = encode_composite_lookup_key(right_join_keys);

                let mut right_records = self.right_source.lookup(
                    from_port,
                    record,
                    &join_key,
                    database,
                    transaction,
                    readers,
                )?;

                for left_record in left_records.iter_mut() {
                    for right_record in right_records.iter_mut() {
                        let join_record = join_records(left_record, right_record);
                        output_records.push(join_record);
                    }
                }
            } else {
                return Err(ExecutionError::InternalStringError(
                    "Invalid constraint key".to_string(),
                ));
            }
        }

        Ok(output_records)
    }

    pub fn insert_index(
        &self,
        key: &[u8],
        value: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.right_index);

        prefix_transaction.put(*database, key, value)?;

        Ok(())
    }

    fn get_lookup_keys(
        &self,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let mut join_keys = vec![];

        let mut exclusive_transaction = transaction.write();
        let right_prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.right_index);

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
}

fn encode_composite_lookup_key(left_join_keys: Vec<Field>) -> Vec<u8> {
    let mut composite_lookup_key = vec![];
    for key in left_join_keys {
        let value = &key.encode();
        let length = value.len() as u32;
        composite_lookup_key.extend_from_slice(&length.to_be_bytes());
        composite_lookup_key.extend_from_slice(value.as_slice());
    }
    composite_lookup_key
}

fn encode_composite_lookup_key_from_record(
    record: &Record,
    merged_join_keys: Vec<usize>,
) -> Vec<u8> {
    let mut composite_lookup_key = vec![];
    for key in merged_join_keys {
        let value = &record.values[key].encode();
        let length = value.len() as u32;
        composite_lookup_key.extend_from_slice(&length.to_be_bytes());
        composite_lookup_key.extend_from_slice(value.as_slice());
    }
    composite_lookup_key
}

fn decode_composite_lookup_key(
    binary_lookup_key: Vec<u8>,
    merged_join_keys: Vec<usize>,
) -> Result<Vec<Field>, DeserializationError> {
    let mut lookup_key = vec![];
    let mut offset = 0;
    for _index in merged_join_keys {
        let length = u32::from_be_bytes([
            binary_lookup_key[offset],
            binary_lookup_key[offset + 1],
            binary_lookup_key[offset + 2],
            binary_lookup_key[offset + 3],
        ]);
        offset += 4;
        let value = &binary_lookup_key[offset..offset + length as usize];
        offset += length as usize;
        lookup_key.push(Field::decode(value)?);
    }
    Ok(lookup_key)
}

fn merge_join_keys(left_join_key: &[usize], right_join_key: &[usize]) -> Vec<usize> {
    let mut merged_join_keys = left_join_key.to_vec();
    merged_join_keys.append(&mut right_join_key.to_vec());
    merged_join_keys
}

fn join_records(left_record: &mut Record, right_record: &mut Record) -> Record {
    left_record.values.append(&mut right_record.values);
    Record::new(None, left_record.values.clone(), None)
}

pub fn get_join_key(record: &Record, join_keys: &[usize]) -> Result<Vec<u8>, TypeError> {
    get_composite_key(record, join_keys)
}

pub fn get_lookup_key(record: &Record, schema: &Schema) -> Result<Vec<u8>, TypeError> {
    let mut lookup_key = Vec::with_capacity(64);
    if let Some(version) = record.version {
        lookup_key.extend_from_slice(&version.to_be_bytes());
    } else {
        lookup_key.extend_from_slice(&[0_u8; 4]);
    }

    let key = get_composite_key(record, schema.primary_index.as_slice())?;
    lookup_key.extend(key);
    Ok(lookup_key)
}

pub fn get_composite_key(record: &Record, key_indexes: &[usize]) -> Result<Vec<u8>, TypeError> {
    let mut join_key = Vec::with_capacity(64);

    for key_index in key_indexes.iter() {
        let key_value = record.get_value(*key_index)?;
        let key_bytes = key_value.encode();
        join_key.extend(key_bytes.iter());
    }

    Ok(join_key)
}
