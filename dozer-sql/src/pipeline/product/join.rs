use std::collections::HashMap;

use dozer_core::{
    node::PortHandle,
    record_store::RecordReader,
    storage::{
        errors::StorageError, lmdb_storage::SharedTransaction,
        prefix_transaction::PrefixTransaction,
    },
};
use dozer_types::{
    errors::types::DeserializationError,
    types::{Field, Record, Schema},
};
use lmdb::Database;

use crate::pipeline::errors::ProductError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinAction {
    Insert,
    Delete,
    // Update,
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
    pub fn execute(
        &self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        match self {
            JoinSource::Table(table) => table.execute(action, from_port, record),
            JoinSource::Join(join) => {
                join.execute(action, from_port, record, database, transaction, readers)
            }
        }
    }

    pub fn lookup(
        &self,
        lookup_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ProductError> {
        match self {
            JoinSource::Table(table) => table.lookup(lookup_key, readers),
            JoinSource::Join(join) => join.lookup(lookup_key, database, transaction, readers),
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
}

impl JoinTable {
    pub fn new(port: PortHandle, schema: Schema) -> Self {
        Self { port, schema }
    }

    pub fn get_source(&self) -> PortHandle {
        self.port
    }

    fn execute(
        &self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        if self.port == from_port {
            if self.schema.primary_index.is_empty() {
                let lookup_key = self.encode_record(record);
                Ok(vec![(action, record.clone(), lookup_key)])
            } else {
                let lookup_key = self.encode_lookup_key(record, &self.schema)?;
                Ok(vec![(action, record.clone(), lookup_key)])
            }
        } else {
            Err(ProductError::InvalidSource(from_port))
        }
    }

    fn lookup(
        &self,
        lookup_key: &[u8],
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ProductError> {
        if self.schema.primary_index.is_empty() {
            let record = self
                .decode_record(lookup_key)
                .map_err(ProductError::DeserializationError)?;
            return Ok(vec![(record, lookup_key.to_vec())]);
        }

        let reader = readers
            .get(&self.port)
            .ok_or(ProductError::HistoryUnavailable(self.port))?;

        let (version, id) = self.decode_lookup_key(lookup_key);

        let mut output_records = vec![];
        if let Some(record) = reader
            .get(&id, version)
            .map_err(|err| ProductError::HistoryRecordNotFound(id, version, self.port, err))?
        {
            output_records.push((record, lookup_key.to_vec()));
        }
        Ok(output_records)
    }

    fn encode_lookup_key(&self, record: &Record, schema: &Schema) -> Result<Vec<u8>, ProductError> {
        let mut lookup_key = Vec::with_capacity(64);
        if let Some(version) = record.version {
            lookup_key.extend_from_slice(&version.to_be_bytes());
        } else {
            lookup_key.extend_from_slice(&[0_u8; 4]);
        }

        for key_index in schema.primary_index.iter() {
            let key_value = record
                .get_value(*key_index)
                .map_err(|e| ProductError::InvalidKey(record.to_owned(), e))?;

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

    fn encode_record(&self, record: &Record) -> Vec<u8> {
        let mut record_bytes = Vec::with_capacity(64);
        if let Some(version) = record.version {
            record_bytes.extend_from_slice(&version.to_be_bytes());
        } else {
            record_bytes.extend_from_slice(&[0_u8; 4]);
        }

        for value in record.values.iter() {
            let value_bytes = value.encode();
            record_bytes.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            record_bytes.extend_from_slice(&value_bytes);
        }
        record_bytes
    }

    fn decode_record(&self, record_bytes: &[u8]) -> Result<Record, DeserializationError> {
        let mut offset = 0;

        let record_version = u32::from_be_bytes([
            record_bytes[offset],
            record_bytes[offset + 1],
            record_bytes[offset + 2],
            record_bytes[offset + 3],
        ]);
        offset += 4;

        let version = if record_version != 0 {
            Some(record_version)
        } else {
            None
        };

        let mut values = vec![];
        while offset < record_bytes.len() {
            let field_length = u32::from_be_bytes([
                record_bytes[offset],
                record_bytes[offset + 1],
                record_bytes[offset + 2],
                record_bytes[offset + 3],
            ]);
            offset += 4;
            let field_bytes = &record_bytes[offset..offset + field_length as usize];
            let value = Field::decode(field_bytes)?;
            values.push(value);
            offset += field_length as usize;
        }
        Ok(Record::new(None, values, version))
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

pub struct JoinBranch {
    pub join_key: Vec<usize>,
    pub source: Box<JoinSource>,
    pub lookup_index: u32,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        schema: Schema,
        left_join_branch: JoinBranch,
        right_join_branch: JoinBranch,
    ) -> Self {
        Self {
            _operator: operator,
            left_join_key: left_join_branch.join_key,
            right_join_key: right_join_branch.join_key,
            schema,
            left_source: left_join_branch.source,
            right_source: right_join_branch.source,
            left_lookup_index: left_join_branch.lookup_index,
            right_lookup_index: right_join_branch.lookup_index,
        }
    }

    pub fn get_sources(&self) -> Vec<PortHandle> {
        [
            self.left_source.get_sources().as_slice(),
            self.right_source.get_sources().as_slice(),
        ]
        .concat()
    }

    pub fn execute(
        &self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        // if the source port is under the left branch of the join
        if self.left_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut left_records = self.left_source.execute(
                action,
                from_port,
                record,
                database,
                transaction,
                readers,
            )?;

            // update left join index
            for (_join_action, left_record, left_lookup_key) in left_records.iter_mut() {
                let left_join_key: Vec<u8> = encode_join_key(left_record, &self.left_join_key);
                self.update_index(
                    _join_action.clone(),
                    &left_join_key,
                    left_lookup_key,
                    self.left_lookup_index,
                    database,
                    transaction,
                )?;

                let join_records = match self._operator {
                    JoinOperatorType::Inner => self.inner_join_left(
                        _join_action.clone(),
                        left_join_key,
                        database,
                        transaction,
                        readers,
                        left_record,
                        left_lookup_key,
                    )?,
                    JoinOperatorType::LeftOuter => self.left_join(
                        _join_action.clone(),
                        left_join_key,
                        database,
                        transaction,
                        readers,
                        left_record,
                        left_lookup_key,
                    )?,
                    JoinOperatorType::RightOuter => self.right_join_reverse(
                        _join_action.clone(),
                        left_join_key,
                        database,
                        transaction,
                        readers,
                        left_record,
                        left_lookup_key,
                    )?,
                };

                output_records.extend(join_records);
            }

            Ok(output_records)
        } else if self.right_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut right_records = self.right_source.execute(
                action,
                from_port,
                record,
                database,
                transaction,
                readers,
            )?;

            // update right join index
            for (_join_action, right_record, right_lookup_key) in right_records.iter_mut() {
                let right_join_key: Vec<u8> = encode_join_key(right_record, &self.right_join_key);
                self.update_index(
                    _join_action.clone(),
                    &right_join_key,
                    right_lookup_key,
                    self.right_lookup_index,
                    database,
                    transaction,
                )?;

                let join_records = match self._operator {
                    JoinOperatorType::Inner => self.inner_join_right(
                        _join_action.clone(),
                        right_join_key,
                        database,
                        transaction,
                        readers,
                        right_record,
                        right_lookup_key,
                    )?,
                    JoinOperatorType::RightOuter => self.right_join(
                        _join_action.clone(),
                        right_join_key,
                        database,
                        transaction,
                        readers,
                        right_record,
                        right_lookup_key,
                    )?,
                    JoinOperatorType::LeftOuter => self.left_join_reverse(
                        _join_action.clone(),
                        right_join_key,
                        database,
                        transaction,
                        readers,
                        right_record,
                        right_lookup_key,
                    )?,
                };
                output_records.extend(join_records);
            }

            return Ok(output_records);
        } else {
            return Err(ProductError::InvalidSource(from_port));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn inner_join_left(
        &self,
        action: JoinAction,
        left_join_key: Vec<u8>,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
        left_record: &mut Record,
        left_lookup_key: &mut [u8],
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        let right_lookup_keys = self.read_index(
            &left_join_key,
            self.right_lookup_index,
            database,
            transaction,
        )?;
        let mut output_records = vec![];

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records =
                self.right_source
                    .lookup(right_lookup_key, database, transaction, readers)?;

            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);

                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    #[allow(clippy::too_many_arguments)]
    fn inner_join_right(
        &self,
        action: JoinAction,
        right_join_key: Vec<u8>,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
        right_record: &mut Record,
        right_lookup_key: &mut [u8],
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        let left_lookup_keys = self.read_index(
            &right_join_key,
            self.left_lookup_index,
            database,
            transaction,
        )?;

        let mut output_records = vec![];
        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records =
                self.left_source
                    .lookup(left_lookup_key, database, transaction, readers)?;

            for (left_record, left_lookup_key) in left_records.iter_mut() {
                // join the records
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);
                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    #[allow(clippy::too_many_arguments)]
    fn left_join(
        &self,
        action: JoinAction,
        left_join_key: Vec<u8>,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
        left_record: &mut Record,
        left_lookup_key: &mut [u8],
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        let right_lookup_keys = self.read_index(
            &left_join_key,
            self.right_lookup_index,
            database,
            transaction,
        )?;
        let mut output_records = vec![];

        if right_lookup_keys.is_empty() {
            // no matching records on the right branch
            let right_record = Record::from_schema(&self.right_source.get_output_schema());
            let join_record = join_records(left_record, &right_record);
            let join_lookup_key = self.encode_join_lookup_key(left_lookup_key, &[]);
            output_records.push((action, join_record, join_lookup_key));

            return Ok(output_records);
        }

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records =
                self.right_source
                    .lookup(right_lookup_key, database, transaction, readers)?;

            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);

                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    #[allow(clippy::too_many_arguments)]
    fn right_join(
        &self,
        action: JoinAction,
        right_join_key: Vec<u8>,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
        right_record: &mut Record,
        right_lookup_key: &mut [u8],
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        let left_lookup_keys = self.read_index(
            &right_join_key,
            self.left_lookup_index,
            database,
            transaction,
        )?;

        let mut output_records = vec![];

        if left_lookup_keys.is_empty() {
            // no matching records on the right branch
            let left_record = Record::from_schema(&self.left_source.get_output_schema());
            let join_record = join_records(&left_record, right_record);
            let join_lookup_key = self.encode_join_lookup_key(right_lookup_key, &[]);
            output_records.push((action, join_record, join_lookup_key));

            return Ok(output_records);
        }

        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records =
                self.left_source
                    .lookup(left_lookup_key, database, transaction, readers)?;

            for (left_record, left_lookup_key) in left_records.iter_mut() {
                // join the records
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);
                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    #[allow(clippy::too_many_arguments)]
    fn right_join_reverse(
        &self,
        action: JoinAction,
        left_join_key: Vec<u8>,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
        left_record: &mut Record,
        left_lookup_key: &mut [u8],
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        let right_lookup_keys = self.read_index(
            &left_join_key,
            self.right_lookup_index,
            database,
            transaction,
        )?;
        let mut output_records = vec![];

        // if there are no matching records on the left branch, no records will be returned
        if right_lookup_keys.is_empty() {
            return Ok(output_records);
        }

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records =
                self.right_source
                    .lookup(right_lookup_key, database, transaction, readers)?;

            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let left_matching_count =
                    self.get_left_matching_count(&action, right_record, database, transaction)?;

                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);

                if left_matching_count > 0 {
                    // if there are multiple matching records on the left branch, the right record will be just returned
                    output_records.push((action.clone(), join_record, join_lookup_key));
                } else {
                    match action {
                        JoinAction::Insert => {
                            let old_join_record = join_records(
                                &Record::from_schema(&self.left_source.get_output_schema()),
                                right_record,
                            );
                            let old_join_lookup_key =
                                self.encode_join_lookup_key(left_lookup_key, &[]);
                            output_records.push((
                                JoinAction::Delete,
                                old_join_record,
                                old_join_lookup_key,
                            ));

                            output_records.push((JoinAction::Insert, join_record, join_lookup_key));
                        }
                        JoinAction::Delete => {
                            let new_join_record = join_records(
                                &Record::from_schema(&self.left_source.get_output_schema()),
                                right_record,
                            );
                            let new_join_lookup_key =
                                self.encode_join_lookup_key(left_lookup_key, &[]);
                            output_records.push((JoinAction::Delete, join_record, join_lookup_key));
                            output_records.push((
                                JoinAction::Insert,
                                new_join_record,
                                new_join_lookup_key,
                            ));
                        }
                    }
                }
            }
        }
        Ok(output_records)
    }

    #[allow(clippy::too_many_arguments)]
    fn left_join_reverse(
        &self,
        action: JoinAction,
        right_join_key: Vec<u8>,
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<u16, Box<dyn RecordReader>>,
        right_record: &mut Record,
        right_lookup_key: &mut [u8],
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        let left_lookup_keys = self.read_index(
            &right_join_key,
            self.left_lookup_index,
            database,
            transaction,
        )?;

        let mut output_records = vec![];

        // if there are no matching records on the left branch, no records will be returned
        if left_lookup_keys.is_empty() {
            return Ok(output_records);
        }

        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records =
                self.left_source
                    .lookup(left_lookup_key, database, transaction, readers)?;

            for (left_record, left_lookup_key) in left_records.iter_mut() {
                let right_matching_count =
                    self.get_right_matching_count(&action, left_record, database, transaction)?;

                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.encode_join_lookup_key(left_lookup_key, right_lookup_key);

                if right_matching_count > 0 {
                    // if there are multiple matching records on the right branch, the left record will be just returned
                    output_records.push((action.clone(), join_record, join_lookup_key));
                } else {
                    match action {
                        JoinAction::Insert => {
                            let old_join_record = join_records(
                                left_record,
                                &Record::from_schema(&self.right_source.get_output_schema()),
                            );
                            let old_join_lookup_key =
                                self.encode_join_lookup_key(left_lookup_key, &[]);

                            // delete the "first left join" record
                            output_records.push((
                                JoinAction::Delete,
                                old_join_record,
                                old_join_lookup_key,
                            ));
                            // insert the new left join record
                            output_records.push((action.clone(), join_record, join_lookup_key));
                        }
                        JoinAction::Delete => {
                            let new_join_record = join_records(
                                left_record,
                                &Record::from_schema(&self.right_source.get_output_schema()),
                            );
                            let new_join_lookup_key =
                                self.encode_join_lookup_key(left_lookup_key, &[]);
                            output_records.push((action.clone(), join_record, join_lookup_key));
                            output_records.push((
                                JoinAction::Insert,
                                new_join_record,
                                new_join_lookup_key,
                            ));
                        }
                    }
                }

                // join the records
                // let join_record = join_records(left_record, right_record);
                // let join_lookup_key =
                //     self.encode_join_lookup_key(left_lookup_key, right_lookup_key);
                // output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    fn get_right_matching_count(
        &self,
        action: &JoinAction,
        left_record: &mut Record,
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<usize, ProductError> {
        let left_join_key: Vec<u8> = encode_join_key(left_record, &self.left_join_key);
        let right_lookup_keys = self.read_index(
            &left_join_key,
            self.right_lookup_index,
            database,
            transaction,
        )?;
        let mut records_count = right_lookup_keys.len();
        if action == &JoinAction::Insert {
            records_count -= 1;
        }
        Ok(records_count)
    }

    fn get_left_matching_count(
        &self,
        action: &JoinAction,
        right_record: &mut Record,
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<usize, ProductError> {
        let right_join_key: Vec<u8> = encode_join_key(right_record, &self.right_join_key);
        let left_lookup_keys = self.read_index(
            &right_join_key,
            self.left_lookup_index,
            database,
            transaction,
        )?;
        let mut records_count = left_lookup_keys.len();
        if action == &JoinAction::Insert {
            records_count -= 1;
        }
        Ok(records_count)
    }

    fn lookup(
        &self,
        lookup_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, ProductError> {
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
        action: JoinAction,
        key: &[u8],
        value: &[u8],
        prefix: u32,
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ProductError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);

        match action {
            JoinAction::Insert => {
                prefix_transaction
                    .put(*database, key, value)
                    .map_err(|err| {
                        ProductError::IndexPutError(key.to_vec(), value.to_vec(), err)
                    })?;
            }
            JoinAction::Delete => {
                prefix_transaction
                    .del(*database, key, Some(value))
                    .map_err(|err| {
                        ProductError::IndexDelError(key.to_vec(), value.to_vec(), err)
                    })?;
            }
        }

        Ok(())
    }

    fn read_index(
        &self,
        join_key: &[u8],
        prefix: u32,
        database: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<Vec<u8>>, ProductError> {
        let mut join_keys = vec![];

        let mut exclusive_transaction = transaction.write();
        let right_prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);

        let cursor = right_prefix_transaction
            .open_cursor(*database)
            .map_err(|err| ProductError::IndexGetError(join_key.to_vec(), err))?;

        if !cursor
            .seek(join_key)
            .map_err(|err| ProductError::IndexGetError(join_key.to_vec(), err))?
        {
            return Ok(join_keys);
        }

        loop {
            let entry = cursor
                .read()
                .map_err(|err| ProductError::IndexGetError(join_key.to_vec(), err))?;

            if entry.is_none() {
                break;
            }

            let (key, value) = entry.unwrap();
            if key != join_key {
                break;
            }

            join_keys.push(value.to_vec());

            if !cursor
                .next()
                .map_err(|err| ProductError::IndexGetError(join_key.to_vec(), err))?
            {
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

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
    Record::new(None, concat_values, None)
}

fn encode_join_key(record: &Record, join_keys: &[usize]) -> Vec<u8> {
    let mut composite_lookup_key = vec![];
    for key in join_keys.iter() {
        let value = &record.values[*key].encode();
        let length = value.len() as u32;
        composite_lookup_key.extend_from_slice(&length.to_be_bytes());
        composite_lookup_key.extend_from_slice(value.as_slice());
    }
    composite_lookup_key
}

// fn join_records(left_record: &Record, right_record: &Record) -> Record {
//     let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
//     let mut left_version = 0;
//     if let Some(version) = left_record.version {
//         left_version = version;
//     }
//     let mut right_version = 0;
//     if let Some(version) = right_record.version {
//         right_version = version;
//     }
//     Record::new(
//         None,
//         concat_values,
//         Some((left_version * 100) + right_version),
//     )
// }

// fn encode_join_key(record: &Record, join_keys: &[usize]) -> Result<Vec<u8>, TypeError> {
//     let mut composite_lookup_key = vec![];
//     let mut version = 0_u32;
//     if let Some(record_version) = &record.version {
//         version = *record_version;
//     }
//     composite_lookup_key.extend_from_slice(&version.to_be_bytes());
//     for key in join_keys.iter() {
//         let value = &record.values[*key].encode();
//         let length = value.len() as u32;
//         composite_lookup_key.extend_from_slice(&length.to_be_bytes());
//         composite_lookup_key.extend_from_slice(value.as_slice());
//     }
//     Ok(composite_lookup_key)
// }
