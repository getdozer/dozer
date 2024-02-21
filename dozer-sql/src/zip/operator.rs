use dozer_types::{
    chrono::{Duration, DurationRound},
    types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
};

use crate::errors::WindowError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordSource {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct ZipOperator {
    index: HashMap<Field, (Option<Record>, Option<Record>)>,
    left_key_index: usize,
    right_key_index: usize,
}

impl ZipOperator {

    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    fn execute(
        &self,
        source: RecordSource,
        record: &Record,

    ) -> Result<Record, ZipError> {
        let table = match record_branch {
            RecordSource::Left => &self.right,
            RecordSource::Right => &self.left,
        };
        
    }

}

