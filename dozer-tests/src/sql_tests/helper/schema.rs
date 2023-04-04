use dozer_sql::sqlparser::ast::ObjectName;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition};

pub fn get_table_name(name: &ObjectName) -> String {
    name.0[0].value.clone()
}

pub fn get_schema(columns: &[rusqlite::Column]) -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: columns
            .iter()
            .map(|c| {
                let typ = c
                    .decl_type()
                    .map_or("string".to_string(), |a| a.to_ascii_lowercase());

                FieldDefinition {
                    name: c
                        .name()
                        .to_string()
                        .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                    typ: match typ.as_str() {
                        "integer" => FieldType::Int,
                        "string" | "text" => FieldType::String,
                        "real" => FieldType::Float,
                        "numeric" => FieldType::Decimal,
                        "timestamp" => FieldType::Timestamp,
                        f => panic!("unknown field_type : {f}"),
                    },
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                }
            })
            .collect(),
        primary_index: vec![0],
    }
}
