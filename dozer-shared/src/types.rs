use std::collections::HashMap;

#[derive(Debug)]
pub enum Field {
    String(String),
    Integer(i32),
    BigInt(i64),
    Float(f32),
    BigFloat(f64),
    Boolean(bool),
    Binary(Vec<u8>),
    Timestamp(u64),
}
// #[derive(Debug)]
// pub struct Schema {
//     id: String,
//     field_names: Vec<String>,
//     field_types: Vec<Field>,
//     // idx: HashMap<String, u16>,
//     // ctr: u16,
// }
//
// #[derive(Debug)]
// pub struct Record {
//     pub values: Vec<Field>,
//     pub schema_id: u32,
// }
