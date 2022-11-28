use dozer_types::types::FieldType;

pub struct ExecutionFieldDef {
    pub name: String,
    pub relation: String,
    pub alias: Option<String>,
    pub typ: FieldType,
}

pub struct ExecutionSchema {
    pub fields: Vec<ExecutionFieldDef>,
}
