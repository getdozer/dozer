use dozer_sql::sqlparser::ast::ObjectName;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};

pub fn get_table_name(name: &ObjectName) -> String {
    name.0[0].value.clone()
}

pub fn get_schema(columns: &[rusqlite::Column]) -> Schema {
    let fields = columns
        .iter()
        .map(|c| {
            let field_type = c
                .decl_type()
                .map_or("string".to_string(), |a| a.to_ascii_lowercase());

            FieldDefinition {
                name: c
                    .name()
                    .to_string()
                    .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                typ: match field_type.as_str() {
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
        .collect();

    let primary_index = columns
        .iter()
        .enumerate()
        //.filter(|(_, c)| c.is_primary_key())
        .map(|(i, _)| i)
        .collect();

    Schema {
        fields,
        primary_index,
    }
}
