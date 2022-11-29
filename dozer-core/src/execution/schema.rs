use dozer_types::types::Schema;

pub struct RelationInfo {
    pub name: String,
    pub aliases: Vec<String>,
}

pub struct FieldInfo {
    pub aliases: Vec<String>,
}

pub struct ExecutionSchema {
    pub source_schema: Schema,
    pub rel_info: RelationInfo,
    pub fields_info: Vec<FieldInfo>,
}
