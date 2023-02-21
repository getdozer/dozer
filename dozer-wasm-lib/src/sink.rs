use std::collections::HashMap;

use dozer_api::grpc::types::{value, Operation, OperationType, Record};

pub struct MemorySink {
    pub records: HashMap<Vec<u8>, Record>,
    pub primary_key: Vec<u32>,
}

impl MemorySink {
    pub fn new(primary_key: Vec<u32>) -> MemorySink {
        MemorySink {
            records: HashMap::new(),
            primary_key,
        }
    }

    pub fn get_primary_key(&self, record: &Record) -> Vec<u8> {
        let keys = self
            .primary_key
            .iter()
            .map(|i| {
                let v = record.values.get(*i as usize).unwrap();

                let v = v.value.as_ref().map(|v| match v {
                    value::Value::UintValue(a) => a.to_be_bytes().to_vec(),
                    value::Value::IntValue(a) => a.to_be_bytes().to_vec(),
                    value::Value::FloatValue(a) => a.to_be_bytes().to_vec(),
                    value::Value::BoolValue(a) => vec![*a as u8],
                    value::Value::StringValue(a) => a.as_bytes().to_vec(),
                    value::Value::BytesValue(a) => a.clone(),
                    value::Value::ArrayValue(_) => todo!(),
                    value::Value::DoubleValue(a) => a.to_be_bytes().to_vec(),
                });
                v.unwrap_or_default()
            })
            .collect::<Vec<Vec<u8>>>();

        keys.join(",".as_bytes()).to_vec()
    }

    pub fn insert(&mut self, record: &Record) {
        let id = self.get_primary_key(&record);
        self.records.insert(id, record.clone());
    }

    pub fn delete(&mut self, record: &Record) {
        let id = self.get_primary_key(&record);
        self.records.remove(&id);
    }

    pub fn update(&mut self, old: &Record, new: &Record) {
        self.insert(old);
        self.insert(new);
    }

    pub fn process(&mut self, op: Operation) {
        match op.typ() {
            OperationType::Insert => {
                let record = op.new.unwrap();
                self.insert(&record);
            }
            OperationType::Delete => {
                let record = op.old.unwrap();
                self.delete(&record);
            }
            OperationType::Update => {
                self.update(&op.old.unwrap(), &op.new.unwrap());
            }
        };
    }
}
