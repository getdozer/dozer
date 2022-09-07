use std::collections::HashMap;

#[derive(Clone)]
pub enum Field {
    string_field(String),
    int_field(i64),
    float_field(f64),
    bool_field(bool),
    binary_field(Vec<u8>),
    timestamp_field(u64),
    empty
}

pub struct Schema {
    id: String,
    field_names: Vec<String>,
    field_types: Vec<Field>,
    idx: HashMap<String, u16>,
    ctr: u16
}


pub struct Record {
    pub values: Vec<Field>,
    pub schema_id: u64
}

pub enum Operation {
    delete {table_id: u64, old: Record},
    insert {table_id: u64, new: Record},
    update {table_id: u64, old: Record, new: Record},
    terminate
}


impl Record {
    pub fn new(schema_id: u64, values: Vec<Field>) -> Record {
        Record {
            schema_id, values
        }
    }
}


impl Schema {

    pub fn new(id: String, field_names: Vec<String>, field_types: Vec<Field>) -> Schema {
        Schema {
            id: id,
            field_names: field_names,
            field_types:field_types,
            idx: HashMap::new(),
            ctr: 0
        }
    }
}





