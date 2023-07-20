use std::{collections::HashMap, sync::Arc};

use crate::types::{Field, FieldDefinition, Record, Schema};

/// To get the `FieldDefinition` or `Field` out of a `Schema` or `Record`, we need two levels of indirection:
///
/// 1. Outer index is to determine if this field is a referenced field in another `Schema`/`Record`, or a direct field.
/// 2. Inner index is to index into the referenced `Schema`/`Record` to get the `FieldDefinition`/`Field`.
///
/// Invariants:
///
/// - If `outer` points to a direct field, `inner` must be 0.
/// - If `outer` points to a referenced `Schema`/`Record`, `inner` must point to a direct field in that `Schema`/`Record`. In other words, we don't allow indirections more than 1 level deep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RefFieldIndex {
    /// The index of the `RefOrFieldDefinition` or `RefOrField` in the `Schema` or `Record`.
    pub outer: u32,
    /// The index of the `FieldDefinition` or `Field` in the `Schema` or `Record`.
    pub inner: u32,
}

#[derive(Debug, Clone)]
pub enum RefOrFieldDefinition {
    Ref {
        /// The referenced `Schema`.
        schema: Arc<RefSchema>,
        /// The indexes of referenced direct fields in the referenced `Schema`.
        direct_field_indexes: Vec<u32>,
    },
    FieldDefinition(FieldDefinition),
}

#[derive(Debug, Default, Clone)]
pub struct RefSchema {
    pub fields: Vec<RefOrFieldDefinition>,
    pub primary_index: Vec<RefFieldIndex>,
}

#[derive(Debug, Clone)]
pub enum RefOrField {
    Ref(Arc<RefRecord>),
    Field(Field),
}

#[derive(Debug, Clone, Default)]
pub struct RefRecord {
    pub values: Vec<RefOrField>,
}

impl RefSchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_field_definition(&self, index: RefFieldIndex) -> &FieldDefinition {
        match &self.fields[index.outer as usize] {
            RefOrFieldDefinition::Ref {
                schema,
                direct_field_indexes,
            } => {
                debug_assert!(
                    direct_field_indexes.contains(&index.inner),
                    "Field index {} is not referenced: referrer: {:?}, referee: {:?}",
                    index.inner,
                    self,
                    schema
                );

                let RefOrFieldDefinition::FieldDefinition(field) = &schema.fields[index.inner as usize] else {
                    panic!("Invariant violated: inner index must point to a direct field in the referenced schema");
                };
                field
            }
            RefOrFieldDefinition::FieldDefinition(field_definition) => {
                debug_assert!(
                    index.inner == 0,
                    "Invariant violated: inner index must be 0 for direct fields"
                );
                field_definition
            }
        }
    }

    pub fn extend_fields(&mut self, referee: Arc<RefSchema>) {
        let mut direct_field_indexes = vec![];
        for (index, ref_or_field_definition) in referee.fields.iter().enumerate() {
            match ref_or_field_definition {
                // Any reference gets copied over as is.
                RefOrFieldDefinition::Ref { .. } => {
                    self.fields.push(ref_or_field_definition.clone())
                }
                // Any direct field gets referenced.
                RefOrFieldDefinition::FieldDefinition(_) => direct_field_indexes.push(index as u32),
            }
        }

        if !direct_field_indexes.is_empty() {
            self.fields.push(RefOrFieldDefinition::Ref {
                schema: referee,
                direct_field_indexes,
            });
        }
    }

    pub fn deref(&self) -> Schema {
        let mut index_mapping = HashMap::new();

        let mut fields = vec![];
        for (outer_index, ref_or_field_definition) in self.fields.iter().enumerate() {
            match ref_or_field_definition {
                RefOrFieldDefinition::Ref {
                    schema,
                    direct_field_indexes,
                } => {
                    for direct_field_index in direct_field_indexes {
                        let RefOrFieldDefinition::FieldDefinition(field_definition) = &schema.fields[*direct_field_index as usize] else {
                            panic!("Invariant violated: inner index must point to a direct field in the referenced schema");
                        };
                        index_mapping.insert(
                            RefFieldIndex {
                                outer: outer_index as u32,
                                inner: *direct_field_index,
                            },
                            fields.len() as u32,
                        );
                        fields.push(field_definition.clone());
                    }
                }
                RefOrFieldDefinition::FieldDefinition(field_definition) => {
                    index_mapping.insert(
                        RefFieldIndex {
                            outer: outer_index as u32,
                            inner: 0,
                        },
                        fields.len() as u32,
                    );
                    fields.push(field_definition.clone());
                }
            }
        }

        let primary_index =
            self.primary_index
                .iter()
                .map(|index| {
                    index_mapping.remove(index).expect(
                        "Invariant violated: primary index must be unique and in the schema",
                    ) as usize
                })
                .collect();

        Schema {
            fields,
            primary_index,
        }
    }
}

impl From<Schema> for RefSchema {
    fn from(schema: Schema) -> Self {
        let mut ref_schema = RefSchema::new();
        for field_definition in schema.fields {
            ref_schema
                .fields
                .push(RefOrFieldDefinition::FieldDefinition(field_definition));
        }
        for primary_index in schema.primary_index {
            ref_schema.primary_index.push(RefFieldIndex {
                outer: primary_index as u32,
                inner: 0,
            });
        }
        ref_schema
    }
}

impl RefRecord {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_field(&self, index: RefFieldIndex) -> &Field {
        match &self.values[index.outer as usize] {
            RefOrField::Ref(record) => {
                let RefOrField::Field(field) = &record.values[index.inner as usize] else {
                    panic!("Invariant violated: inner index must point to a direct field in the referenced record");
                };
                field
            }
            RefOrField::Field(field) => {
                debug_assert!(
                    index.inner == 0,
                    "Invariant violated: inner index must be 0 for direct fields"
                );
                field
            }
        }
    }

    pub fn extend(&mut self, record: Arc<RefRecord>) {
        let mut record_should_be_referenced = false;
        for ref_or_field in &record.values {
            if let RefOrField::Ref(_) = ref_or_field {
                // Any reference gets copied over as is.
                self.values.push(ref_or_field.clone());
            } else {
                record_should_be_referenced = true;
            }
        }

        if record_should_be_referenced {
            self.values.push(RefOrField::Ref(record));
        }
    }

    pub fn deref(&self, schema: &RefSchema) -> Record {
        let mut values = vec![];

        for (ref_or_field, ref_or_field_definition) in self.values.iter().zip(&schema.fields) {
            match (ref_or_field, ref_or_field_definition) {
                (
                    RefOrField::Ref(record),
                    RefOrFieldDefinition::Ref {
                        direct_field_indexes,
                        ..
                    },
                ) => {
                    for direct_field_index in direct_field_indexes {
                        let RefOrField::Field(field) = &record.values[*direct_field_index as usize] else {
                            panic!("Invariant violated: inner index must point to a direct field in the referenced record");
                        };
                        values.push(field.clone());
                    }
                }
                (RefOrField::Field(field), RefOrFieldDefinition::FieldDefinition(_)) => {
                    values.push(field.clone());
                }
                _ => panic!("Record and schema must match"),
            }
        }

        Record {
            values,
            lifetime: None,
        }
    }
}

impl From<Record> for RefRecord {
    fn from(record: Record) -> Self {
        let mut ref_record = RefRecord::new();
        for field in record.values {
            ref_record.values.push(RefOrField::Field(field));
        }
        ref_record
    }
}
