#![allow(dead_code)]
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::dag::node::PortHandle;
use dozer_core::storage::errors::StorageError::SerializationError;
use dozer_types::types::{Field, Operation, Record};

pub struct RelationIndex {
    id: PortHandle,
    //db: IndexedDatabase,
    pk_index: Vec<usize>,
    fk_indexes: Vec<(u16, Expression)>,
    vals_indexes: Vec<usize>,
}

struct FieldData {
    primary_key: Vec<u8>,
    foreign_keys: Vec<(u16, Vec<u8>)>,
    fields: Vec<u8>,
}

impl RelationIndex {
    pub fn new(
        id: PortHandle,
        //db: IndexedDatabase,
        pk_index: Vec<usize>,
        fk_indexes: Vec<(u16, Expression)>,
        vals_indexes: Vec<usize>,
    ) -> Self {
        Self {
            id,
            //db,
            pk_index,
            fk_indexes,
            vals_indexes,
        }
    }

    fn extract(&self, rec: &Record) -> Result<FieldData, PipelineError> {
        let pk_val = rec.get_key(&self.pk_index)?;

        let mut sec_indexes = Vec::<(u16, Vec<u8>)>::with_capacity(self.fk_indexes.len());
        for fk_idx in &self.fk_indexes {
            let val = fk_idx.1.evaluate(rec)?.to_bytes()?;
            sec_indexes.push((fk_idx.0, val));
        }

        let mut out_fields = Vec::<&Field>::with_capacity(self.vals_indexes.len());
        for i in &self.vals_indexes {
            out_fields.push(rec.get_value(*i)?);
        }
        let out_fields_val = bincode::serialize(&out_fields).map_err(|e| SerializationError {
            typ: "Vec<Field>".to_string(),
            reason: Box::new(e),
        })?;

        Ok(FieldData {
            primary_key: pk_val,
            foreign_keys: sec_indexes,
            fields: out_fields_val,
        })
    }

    pub fn handle(&self, op: Operation) -> Result<(), PipelineError> {
        match op {
            Operation::Update { old: _, new: _ } => {}
            Operation::Delete { old: _ } => {}
            Operation::Insert { new } => {
                let _fdata = self.extract(&new)?;
            }
        }

        Ok(())
    }
}
