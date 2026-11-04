use std::{
    collections::HashMap,
    mem::{discriminant, Discriminant},
};

use dozer_core::{
    channels::ProcessorChannelForwarder, epoch::Epoch, node::Processor, DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    errors::internal::BoxedError,
    types::{Field, Operation, Record, Schema, TableOperation},
};

use super::plan::{equal, Predicate};
use crate::errors::PipelineError;

#[derive(Clone, Debug, Default)]
pub(crate) struct Membership {
    values: HashMap<Field, usize>,
    /// Number of distinct retained values per canonical class.
    classes: HashMap<Discriminant<Field>, usize>,
    nulls: usize,
    total: usize,
}

impl Membership {
    pub(crate) fn contains(&self, value: Field) -> Result<Field, PipelineError> {
        if self.total == 0 {
            return Ok(Field::Boolean(false));
        }
        if value == Field::Null {
            return Ok(Field::Null);
        }
        let value = canonical(value);
        if self.values.contains_key(&value) {
            return Ok(Field::Boolean(true));
        }
        let mut unknown = self.nulls != 0;
        // Values of the same class are fully decided by the hash lookup above; only
        // candidates of another class (e.g. an integer against a decimal or a string)
        // need the generic comparison.
        let class = discriminant(&value);
        let has_other_class = self.classes.keys().any(|other| *other != class);
        if !has_other_class {
            return Ok(if unknown {
                Field::Null
            } else {
                Field::Boolean(false)
            });
        }
        for candidate in self
            .values
            .keys()
            .filter(|candidate| discriminant(*candidate) != class)
        {
            match equal(value.clone(), candidate.clone())? {
                Field::Boolean(true) => return Ok(Field::Boolean(true)),
                Field::Null => unknown = true,
                _ => {}
            }
        }
        Ok(if unknown {
            Field::Null
        } else {
            Field::Boolean(false)
        })
    }

    fn insert(&mut self, record: &Record) -> Result<(), PipelineError> {
        let value = single_value(record)?;
        self.total = increment(self.total)?;
        if value == &Field::Null {
            self.nulls = increment(self.nulls)?;
        } else {
            let key = canonical(value.clone());
            let class = discriminant(&key);
            let count = self.values.entry(key).or_default();
            if *count == 0 {
                *self.classes.entry(class).or_default() += 1;
            }
            *count = increment(*count)?;
        }
        Ok(())
    }

    fn delete(&mut self, record: &Record) -> Result<(), PipelineError> {
        let value = single_value(record)?;
        if value == &Field::Null {
            self.nulls = self.nulls.checked_sub(1).ok_or_else(missing_record)?;
        } else {
            let key = canonical(value.clone());
            let count = self.values.get_mut(&key).ok_or_else(missing_record)?;
            *count -= 1;
            if *count == 0 {
                self.values.remove(&key);
                let class = discriminant(&key);
                if let Some(distinct) = self.classes.get_mut(&class) {
                    *distinct -= 1;
                    if *distinct == 0 {
                        self.classes.remove(&class);
                    }
                }
            }
        }
        self.total -= 1;
        Ok(())
    }

    fn same_membership(&self, other: &Self) -> bool {
        (self.total == 0) == (other.total == 0)
            && (self.nulls == 0) == (other.nulls == 0)
            && self.values.len() == other.values.len()
            && self
                .values
                .keys()
                .all(|value| other.values.contains_key(value))
    }
}

/// Maps values that compare equal within a class onto one hashable key:
/// all integer widths become `I128` and `String` becomes `Text`.
fn canonical(value: Field) -> Field {
    match value {
        Field::Int(v) => Field::I128(v as i128),
        Field::Int8(v) => Field::I128(v as i128),
        Field::UInt(v) => Field::I128(v as i128),
        Field::U128(v) => i128::try_from(v).map(Field::I128).unwrap_or(Field::U128(v)),
        Field::String(v) => Field::Text(v),
        other => other,
    }
}

fn single_value(record: &Record) -> Result<&Field, PipelineError> {
    if record.values.len() != 1 {
        return Err(PipelineError::InvalidQuery(
            "IN subquery record must contain exactly one column".into(),
        ));
    }
    Ok(&record.values[0])
}

fn missing_record() -> PipelineError {
    PipelineError::InvalidQuery("Dynamic IN received a retraction for an absent record".into())
}

fn increment(count: usize) -> Result<usize, PipelineError> {
    count
        .checked_add(1)
        .ok_or_else(|| PipelineError::InvalidQuery("Dynamic IN multiplicity overflow".into()))
}

#[derive(Clone, Debug)]
struct OuterRow {
    count: usize,
    selected: bool,
}

#[derive(Debug)]
pub(crate) struct DynamicSelectionProcessor {
    predicate: Predicate,
    schema: Schema,
    outer: HashMap<Record, OuterRow>,
    inputs: Vec<Membership>,
}

impl DynamicSelectionProcessor {
    pub(crate) fn new(predicate: Predicate, schema: Schema, input_count: usize) -> Self {
        Self {
            predicate,
            schema,
            outer: HashMap::new(),
            inputs: vec![Membership::default(); input_count],
        }
    }

    fn selected(&mut self, record: &Record) -> Result<bool, PipelineError> {
        if record.values.len() != self.schema.fields.len() {
            return Err(PipelineError::InvalidQuery(
                "Dynamic IN outer record does not match its schema".into(),
            ));
        }
        Ok(self
            .predicate
            .evaluate(record, &self.schema, &self.inputs)?
            == Field::Boolean(true))
    }

    fn process_outer(&mut self, op: Operation) -> Result<Option<Operation>, PipelineError> {
        match op {
            Operation::Insert { new } => {
                let selected = self.selected(&new)?;
                let count = increment(self.outer.get(&new).map_or(0, |row| row.count))?;
                self.outer.insert(new.clone(), OuterRow { count, selected });
                Ok(selected.then_some(Operation::Insert { new }))
            }
            Operation::Delete { old } => {
                let row = self.outer.get_mut(&old).ok_or_else(missing_record)?;
                let selected = row.selected;
                row.count -= 1;
                if row.count == 0 {
                    self.outer.remove(&old);
                }
                Ok(selected.then_some(Operation::Delete { old }))
            }
            Operation::Update { old, new } => {
                let old_selected = self.outer.get(&old).ok_or_else(missing_record)?.selected;
                let new_selected = self.selected(&new)?;
                // Validate the entire update before mutating or emitting either half.
                let count = if old == new {
                    self.outer[&old].count
                } else {
                    increment(self.outer.get(&new).map_or(0, |row| row.count))?
                };
                let old_row = self.outer.get_mut(&old).ok_or_else(missing_record)?;
                old_row.count -= 1;
                if old_row.count == 0 {
                    self.outer.remove(&old);
                }
                self.outer.insert(
                    new.clone(),
                    OuterRow {
                        count,
                        selected: new_selected,
                    },
                );
                Ok(match (old_selected, new_selected) {
                    (true, true) => Some(Operation::Update { old, new }),
                    (true, false) => Some(Operation::Delete { old }),
                    (false, true) => Some(Operation::Insert { new }),
                    (false, false) => None,
                })
            }
            Operation::BatchInsert { new } => {
                let mut additions: HashMap<Record, OuterRow> = HashMap::new();
                let mut selected = vec![];
                for record in new {
                    let fulfilled = self.selected(&record)?;
                    let count = additions
                        .get(&record)
                        .or_else(|| self.outer.get(&record))
                        .map_or(0, |row| row.count);
                    additions.insert(
                        record.clone(),
                        OuterRow {
                            count: increment(count)?,
                            selected: fulfilled,
                        },
                    );
                    if fulfilled {
                        selected.push(record);
                    }
                }
                self.outer.extend(additions);
                Ok((!selected.is_empty()).then_some(Operation::BatchInsert { new: selected }))
            }
        }
    }

    fn process_inner(
        &mut self,
        index: usize,
        op: Operation,
    ) -> Result<Vec<Operation>, PipelineError> {
        let mut next = self.inputs.clone();
        let input = &mut next[index];
        match op {
            Operation::Insert { new } => input.insert(&new)?,
            Operation::Delete { old } => input.delete(&old)?,
            Operation::Update { old, new } => {
                input.delete(&old)?;
                input.insert(&new)?;
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    input.insert(&record)?;
                }
            }
        }
        let mut changes = vec![];
        let mut outputs = vec![];
        if !self.inputs[index].same_membership(input) {
            // General compound predicates and NULL/empty boundaries can affect every
            // retained row. Only presence changes require this pass; duplicate count
            // changes and same-value updates produce no transient retractions.
            for (record, row) in &self.outer {
                let selected =
                    self.predicate.evaluate(record, &self.schema, &next)? == Field::Boolean(true);
                if selected != row.selected {
                    for _ in 0..row.count {
                        outputs.push(if selected {
                            Operation::Insert {
                                new: record.clone(),
                            }
                        } else {
                            Operation::Delete {
                                old: record.clone(),
                            }
                        });
                    }
                    changes.push((record.clone(), selected));
                }
            }
        }
        self.inputs = next;
        for (record, selected) in changes {
            self.outer.get_mut(&record).unwrap().selected = selected;
        }
        Ok(outputs)
    }
}

impl Processor for DynamicSelectionProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let outputs = if op.port == DEFAULT_PORT_HANDLE {
            self.process_outer(op.op)?.into_iter().collect()
        } else {
            let index = usize::from(op.port)
                .checked_sub(1)
                .ok_or(PipelineError::InvalidPortHandle(op.port))?;
            if index >= self.inputs.len() {
                return Err(PipelineError::InvalidPortHandle(op.port).into());
            }
            self.process_inner(index, op.op)?
        };
        for output in outputs {
            fw.send(TableOperation {
                id: op.id,
                op: output,
                port: DEFAULT_PORT_HANDLE,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod membership_tests {
    use super::*;

    fn record(value: Field) -> Record {
        Record::new(vec![value])
    }

    #[test]
    fn lookup_uses_canonical_keys_with_many_values() {
        let mut membership = Membership::default();
        for i in 0..50_000i64 {
            membership.insert(&record(Field::Int(i))).unwrap();
        }
        // A single class means no value is ever compared with the generic
        // equality: every lookup is a hash probe.
        assert_eq!(membership.classes.len(), 1);
        for i in (0..50_000u64).step_by(997) {
            assert_eq!(
                membership.contains(Field::UInt(i)).unwrap(),
                Field::Boolean(true)
            );
        }
        assert_eq!(
            membership.contains(Field::I128(-1)).unwrap(),
            Field::Boolean(false)
        );
        membership.insert(&record(Field::Null)).unwrap();
        assert_eq!(membership.contains(Field::Int(-1)).unwrap(), Field::Null);
    }

    #[test]
    fn strings_match_text_and_retraction_clears_classes() {
        let mut membership = Membership::default();
        membership.insert(&record(Field::String("a".into()))).unwrap();
        assert_eq!(
            membership.contains(Field::Text("a".into())).unwrap(),
            Field::Boolean(true)
        );
        membership.delete(&record(Field::Text("a".into()))).unwrap();
        assert!(membership.classes.is_empty());
        assert_eq!(
            membership.contains(Field::Text("a".into())).unwrap(),
            Field::Boolean(false)
        );
    }
}
