use dozer_core::node::PortHandle;
use dozer_types::types::{Field, Record, Schema};

use multimap::MultiMap;

use crate::pipeline::errors::JoinError;

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
        &mut self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        match self {
            JoinSource::Table(table) => table.execute(action, from_port, record),
            JoinSource::Join(join) => join.execute(action, from_port, record),
        }
    }

    pub fn lookup(&self, lookup_key: &[Field]) -> Result<Vec<(Record, Vec<Field>)>, JoinError> {
        match self {
            JoinSource::Table(table) => table.lookup(lookup_key),
            JoinSource::Join(join) => join.lookup(lookup_key),
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
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        debug_assert!(self.port == from_port);

        let lookup_key = record.values.clone();
        Ok(vec![(action, record.clone(), lookup_key)])
    }

    fn lookup(&self, lookup_key: &[Field]) -> Result<Vec<(Record, Vec<Field>)>, JoinError> {
        let record = Record::new(None, lookup_key.to_vec(), None);
        Ok(vec![(record, lookup_key.to_vec())])
    }
}

#[derive(Clone, Debug)]
pub struct JoinOperator {
    _operator: JoinOperatorType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    schema: Schema,

    left_source: Box<JoinSource>,
    right_source: Box<JoinSource>,

    left_lookup_index_map: MultiMap<Vec<Field>, Vec<Field>>,
    right_lookup_index_map: MultiMap<Vec<Field>, Vec<Field>>,
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
            left_join_key_indexes: left_join_branch.join_key,
            right_join_key_indexes: right_join_branch.join_key,
            schema,
            left_source: left_join_branch.source,
            right_source: right_join_branch.source,
            left_lookup_index_map: MultiMap::new(),
            right_lookup_index_map: MultiMap::new(),
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
        &mut self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        // if the source port is under the left branch of the join
        if self.left_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut left_records = self.left_source.execute(action, from_port, record)?;

            // update left join index
            for (_join_action, left_record, left_lookup_key) in left_records.iter_mut() {
                let left_join_key = get_join_key_fields(left_record, &self.left_join_key_indexes);
                self.update_left_index(_join_action.clone(), &left_join_key, left_lookup_key)?;

                let join_records = match self._operator {
                    JoinOperatorType::Inner => self.inner_join_left(
                        _join_action.clone(),
                        left_join_key,
                        left_record,
                        left_lookup_key,
                    )?,
                    JoinOperatorType::LeftOuter => self.left_join(
                        _join_action.clone(),
                        left_join_key,
                        left_record,
                        left_lookup_key,
                    )?,
                    JoinOperatorType::RightOuter => self.right_join_reverse(
                        _join_action.clone(),
                        left_join_key,
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
            let mut right_records = self.right_source.execute(action, from_port, record)?;

            // update right join index
            for (_join_action, right_record, right_lookup_key) in right_records.iter_mut() {
                let right_join_key =
                    get_join_key_fields(right_record, &self.right_join_key_indexes);
                self.update_right_index(_join_action.clone(), &right_join_key, right_lookup_key)?;

                let join_records = match self._operator {
                    JoinOperatorType::Inner => self.inner_join_right(
                        _join_action.clone(),
                        right_join_key,
                        right_record,
                        right_lookup_key,
                    )?,
                    JoinOperatorType::RightOuter => self.right_join(
                        _join_action.clone(),
                        right_join_key,
                        right_record,
                        right_lookup_key,
                    )?,
                    JoinOperatorType::LeftOuter => self.left_join_reverse(
                        _join_action.clone(),
                        right_join_key,
                        right_record,
                        right_lookup_key,
                    )?,
                };
                output_records.extend(join_records);
            }

            return Ok(output_records);
        } else {
            return Err(JoinError::InvalidSource(from_port));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn inner_join_left(
        &self,
        action: JoinAction,
        left_join_key: Vec<Field>,
        left_record: &mut Record,
        left_lookup_key: &mut [Field],
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records = self.right_source.lookup(right_lookup_key)?;

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
        right_join_key: Vec<Field>,
        right_record: &mut Record,
        right_lookup_key: &mut [Field],
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];
        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records = self.left_source.lookup(left_lookup_key)?;

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
        left_join_key: Vec<Field>,
        left_record: &mut Record,
        left_lookup_key: &mut [Field],
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

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
            let mut right_records = self.right_source.lookup(right_lookup_key)?;

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
        right_join_key: Vec<Field>,
        right_record: &mut Record,
        right_lookup_key: &mut [Field],
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

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
            let mut left_records = self.left_source.lookup(left_lookup_key)?;

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
        left_join_key: Vec<Field>,
        left_record: &mut Record,
        left_lookup_key: &mut [Field],
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        // if there are no matching records on the left branch, no records will be returned
        if right_lookup_keys.is_empty() {
            return Ok(output_records);
        }

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records = self.right_source.lookup(right_lookup_key)?;

            for (right_record, right_lookup_key) in right_records.iter_mut() {
                let left_matching_count = self.get_left_matching_count(&action, right_record)?;

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
        right_join_key: Vec<Field>,
        right_record: &mut Record,
        right_lookup_key: &mut [Field],
    ) -> Result<Vec<(JoinAction, Record, Vec<Field>)>, JoinError> {
        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        // if there are no matching records on the left branch, no records will be returned
        if left_lookup_keys.is_empty() {
            return Ok(output_records);
        }

        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records = self.left_source.lookup(left_lookup_key)?;

            for (left_record, left_lookup_key) in left_records.iter_mut() {
                let right_matching_count = self.get_right_matching_count(&action, left_record)?;

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
    ) -> Result<usize, JoinError> {
        let left_join_key: Vec<Field> =
            get_join_key_fields(left_record, &self.left_join_key_indexes);

        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

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
    ) -> Result<usize, JoinError> {
        let right_join_key = get_join_key_fields(right_record, &self.right_join_key_indexes);

        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut records_count = left_lookup_keys.len();
        if action == &JoinAction::Insert {
            records_count -= 1;
        }
        Ok(records_count)
    }

    fn lookup(&self, lookup_key: &[Field]) -> Result<Vec<(Record, Vec<Field>)>, JoinError> {
        let mut output_records = vec![];

        let (left_loookup_key, right_lookup_key) = self.decode_join_lookup_key(lookup_key);

        let mut left_records = self.left_source.lookup(&left_loookup_key)?;

        let mut right_records = self.right_source.lookup(&right_lookup_key)?;

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

    pub fn update_left_index(
        &mut self,
        action: JoinAction,
        key: &[Field],
        value: &[Field],
    ) -> Result<(), JoinError> {
        match action {
            JoinAction::Insert => {
                self.left_lookup_index_map
                    .insert(key.to_vec(), value.to_vec());
            }
            JoinAction::Delete => {
                self.left_lookup_index_map.remove(key);
            }
        }

        Ok(())
    }

    pub fn update_right_index(
        &mut self,
        action: JoinAction,
        key: &[Field],
        value: &[Field],
    ) -> Result<(), JoinError> {
        match action {
            JoinAction::Insert => {
                self.right_lookup_index_map
                    .insert(key.to_vec(), value.to_vec());
            }
            JoinAction::Delete => {
                self.right_lookup_index_map.remove(key);
            }
        }

        Ok(())
    }

    fn encode_join_lookup_key(
        &self,
        left_lookup_key: &[Field],
        right_lookup_key: &[Field],
    ) -> Vec<Field> {
        [left_lookup_key, right_lookup_key].concat()
    }

    fn decode_join_lookup_key(&self, join_lookup_key: &[Field]) -> (Vec<Field>, Vec<Field>) {
        let left_schema_len = self.left_source.get_output_schema().fields.len();
        let right_schema_len = self.right_source.get_output_schema().fields.len();

        debug_assert!(join_lookup_key.len() == left_schema_len + right_schema_len);

        let (left_lookup_key, right_lookup_key) = join_lookup_key.split_at(left_schema_len);

        debug_assert!(left_lookup_key.len() == left_schema_len);
        debug_assert!(right_lookup_key.len() == right_schema_len);

        (left_lookup_key.to_vec(), right_lookup_key.to_vec())
    }
}

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
    Record::new(None, concat_values, None)
}

fn get_join_key_fields(record: &Record, join_keys: &[usize]) -> Vec<Field> {
    let mut composite_lookup_key = vec![];
    for key in join_keys.iter() {
        let value = &record.values[*key];
        composite_lookup_key.push(value.clone());
    }
    composite_lookup_key
}
