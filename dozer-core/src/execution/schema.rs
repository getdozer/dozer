use dozer_types::types::Schema;

pub struct FieldInfo {
    pub relation: String,
    pub aliases: Vec<String>,
}

pub struct ExecutionSchema {
    pub source_schema: Schema,
    pub fields_info: Vec<FieldInfo>,
}
