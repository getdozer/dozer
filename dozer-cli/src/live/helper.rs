use dozer_types::grpc_types::{conversions::field_definition_to_grpc, live::Schema};

pub fn map_schema(schema: dozer_types::types::Schema) -> Schema {
    Schema {
        primary_index: schema.primary_index.into_iter().map(|i| i as i32).collect(),
        fields: field_definition_to_grpc(schema.fields),
    }
}
