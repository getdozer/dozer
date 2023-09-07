use std::collections::HashMap;

use dozer_types::types::Field;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Insert { new: Vec<Field> },
    Update { old: Vec<Field>, new: Vec<Field> },
    Delete { old: Vec<Field> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Records {
    primary_index: Vec<usize>,
    data: HashMap<Vec<Field>, Vec<Field>>,
}

impl Records {
    pub fn new(primary_index: Vec<usize>) -> Self {
        Self {
            primary_index,
            data: HashMap::new(),
        }
    }

    pub fn append_operation(&mut self, operation: Operation) {
        match operation {
            Operation::Insert { new } => {
                let primary_key = get_primary_key(&new, &self.primary_index);
                assert!(self.data.insert(primary_key, new).is_none());
            }
            Operation::Update { old, new } => {
                let old_primary_key = get_primary_key(&old, &self.primary_index);
                assert!(self.data.remove(&old_primary_key).is_some());
                let new_primary_key = get_primary_key(&new, &self.primary_index);
                assert!(self.data.insert(new_primary_key, new).is_none());
            }
            Operation::Delete { old } => {
                let primary_key = get_primary_key(&old, &self.primary_index);
                assert!(self.data.remove(&primary_key).is_some());
            }
        }
    }
}

fn get_primary_key(record: &[Field], primary_index: &[usize]) -> Vec<Field> {
    primary_index.iter().map(|i| record[*i].clone()).collect()
}
