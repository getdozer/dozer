use std::collections::HashMap;

pub enum Field {
    string_field(String),
    integer_field(i32),
    bigint_field(i64),
    float_field(f32),
    bigfloat_field(f64),
    boolean_field(bool),
    binary_field(Vec<u8>),
    timestamp_field(u64)
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





