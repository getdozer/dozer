use std::hash::Hash;
use std::sync::Arc;

use super::{Field, Lifetime, Operation, Record, Schema};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ProcessorRecord {
    /// List of values, following the definitions of `fields` of the associated schema
    values: Vec<RefOrField>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    pub lifetime: Option<Lifetime>,

    // Indexes of the fields to be used if this is a reference record
    index: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq, Hash)]

pub enum RefOrField {
    Ref(ProcessorRecordRef),
    Field(Field),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessorRecordRef(pub Arc<ProcessorRecord>);

impl ProcessorRecordRef {
    pub fn new(record: ProcessorRecord) -> Self {
        ProcessorRecordRef(Arc::new(record))
    }

    pub fn get_record(&self) -> &ProcessorRecord {
        &self.0
    }
}

impl From<Record> for ProcessorRecord {
    fn from(record: Record) -> Self {
        let mut ref_record = ProcessorRecord::new();
        for field in record.values {
            ref_record.extend_direct_field(field);
        }
        ref_record
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// A CDC event.
pub enum ProcessorOperation {
    Delete {
        old: ProcessorRecordRef,
    },
    Insert {
        new: ProcessorRecordRef,
    },
    Update {
        old: ProcessorRecordRef,
        new: ProcessorRecordRef,
    },
}

impl From<Operation> for ProcessorOperation {
    fn from(record: Operation) -> Self {
        match record {
            Operation::Delete { old } => ProcessorOperation::Delete {
                old: ProcessorRecordRef::new(old.into()),
            },
            Operation::Insert { new } => ProcessorOperation::Insert {
                new: ProcessorRecordRef::new(new.into()),
            },
            Operation::Update { old, new } => ProcessorOperation::Update {
                old: ProcessorRecordRef::new(old.into()),
                new: ProcessorRecordRef::new(new.into()),
            },
        }
    }
}

impl ProcessorOperation {
    pub fn clone_deref(&self) -> Operation {
        match self {
            ProcessorOperation::Delete { old } => Operation::Delete {
                old: old.0.clone_deref(),
            },
            ProcessorOperation::Insert { new } => Operation::Insert {
                new: new.0.clone_deref(),
            },
            ProcessorOperation::Update { old, new } => Operation::Update {
                old: old.0.clone_deref(),
                new: new.0.clone_deref(),
            },
        }
    }
}

impl ProcessorRecord {
    pub fn new() -> Self {
        ProcessorRecord {
            values: Vec::new(),
            lifetime: None,
            index: Vec::new(),
        }
    }

    pub fn clone_deref(&self) -> Record {
        let mut values: Vec<Field> = Vec::new();
        for field in self.get_fields() {
            values.push(field.clone());
        }
        let mut record = Record::new(values);
        record.set_lifetime(self.get_lifetime());
        record
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.clone()
    }
    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime;
    }

    pub fn get_field_count(&self) -> usize {
        self.index.len()
    }

    pub fn extend_referenced_fields(
        &mut self,
        other: ProcessorRecordRef,
        field_indexes: &Vec<usize>,
    ) {
        // Count each referenced record field length and increment the index cumulatively
        let curr_index = self.get_field_count();

        self.values.push(RefOrField::Ref(other));

        for idx in field_indexes {
            self.index.push(curr_index as u32 + *idx as u32);
        }
    }

    pub fn extend_direct_field(&mut self, field: Field) {
        self.values.push(RefOrField::Field(field));
        self.index.push(self.get_field_count() as u32);
    }

    pub fn get_fields(&self) -> Vec<&Field> {
        let mut fields = Vec::new();
        for idx in &self.index {
            let field = self.get_field_by_index(*idx as usize);

            fields.push(field);
        }
        fields
    }
    // Function to get a field by its index
    pub fn get_field_by_index(&self, index: usize) -> &Field {
        let mut current_index = index;

        // Iterate through the values and update the counts
        for field_or_ref in self.values.iter() {
            match field_or_ref {
                RefOrField::Ref(record_ref) => {
                    // If it's a reference, check if it matches the given index
                    let rec = record_ref.get_record();
                    let count = rec.get_field_count();
                    if current_index < count {
                        return rec.get_field_by_index(current_index);
                    }
                    current_index -= count;
                }
                RefOrField::Field(field) => {
                    // If it's a field, check if it matches the given index
                    if current_index == 0 {
                        return field;
                    }
                    current_index -= 1;
                }
            }
        }
        panic!("Field is expected with the index: {}", index);
    }

    pub fn get_key(&self, indexes: &Vec<usize>) -> Vec<u8> {
        debug_assert!(!indexes.is_empty(), "Primary key indexes cannot be empty");

        let mut tot_size = 0_usize;
        let mut buffers = Vec::<Vec<u8>>::with_capacity(indexes.len());
        for i in indexes {
            let bytes = self.get_field_by_index(*i).encode();
            tot_size += bytes.len();
            buffers.push(bytes);
        }

        let mut res_buffer = Vec::<u8>::with_capacity(tot_size);
        for i in buffers {
            res_buffer.extend(i);
        }
        res_buffer
    }

    pub fn nulls_from_schema(schema: &Schema) -> ProcessorRecord {
        Self::nulls(schema.fields.len())
    }

    pub fn nulls(size: usize) -> ProcessorRecord {
        ProcessorRecord {
            values: (0..size).map(|_| RefOrField::Field(Field::Null)).collect(),
            lifetime: None,
            index: Vec::new(),
        }
    }
}
