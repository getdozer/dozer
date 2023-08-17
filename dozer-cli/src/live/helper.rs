use dozer_api::grpc::types_helper::map_field_definitions;
use dozer_types::grpc_types::live::Schema;

pub fn map_schema(schema: dozer_types::types::Schema) -> Schema {
    Schema {
        primary_index: schema.primary_index.into_iter().map(|i| i as i32).collect(),
        fields: map_field_definitions(schema.fields),
    }
}
