use dozer_types::{
    models::endpoint::{
        ApiEndpoint, FullText, SecondaryIndex, SecondaryIndexConfig, SortedInverted,
    },
    types::{FieldDefinition, FieldType, IndexDefinition, Schema, SchemaWithIndex},
};

use crate::errors::BuildError;

pub fn modify_schema(
    table_name: &str,
    schema: &Schema,
    api_endpoint: &ApiEndpoint,
) -> Result<SchemaWithIndex, BuildError> {
    let mut schema = schema.clone();
    // Generated Cache index based on api_index
    let configured_index = create_primary_indexes(&schema.fields, &api_endpoint.index.primary_key)?;
    // Generated schema in SQL
    let upstream_index = schema.primary_index.clone();

    let index = match (configured_index.is_empty(), upstream_index.is_empty()) {
        (true, true) => vec![],
        (true, false) => upstream_index,
        (false, true) => configured_index,
        (false, false) => {
            if !upstream_index.eq(&configured_index) {
                return Err(BuildError::MismatchPrimaryKey {
                    table_name: table_name.to_string(),
                    expected: get_field_names(&schema, &upstream_index),
                    actual: get_field_names(&schema, &configured_index),
                });
            }
            configured_index
        }
    };

    schema.primary_index = index;

    let secondary_indexes =
        generate_secondary_indexes(&schema.fields, &api_endpoint.index.secondary)?;

    Ok((schema, secondary_indexes))
}

fn get_field_names(schema: &Schema, indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|idx| schema.fields[*idx].name.to_owned())
        .collect()
}

fn create_primary_indexes(
    field_definitions: &[FieldDefinition],
    primary_key: &[String],
) -> Result<Vec<usize>, BuildError> {
    primary_key
        .iter()
        .map(|name| field_index_from_field_name(field_definitions, name))
        .collect()
}

fn generate_secondary_indexes(
    field_definitions: &[FieldDefinition],
    config: &SecondaryIndexConfig,
) -> Result<Vec<IndexDefinition>, BuildError> {
    let mut result = vec![];

    // Create default indexes unless skipped.
    for (index, field) in field_definitions.iter().enumerate() {
        if config.skip_default.contains(&field.name) {
            continue;
        }

        match field.typ {
            // Create sorted inverted indexes for these fields
            FieldType::UInt
            | FieldType::U128
            | FieldType::Int
            | FieldType::I128
            | FieldType::Float
            | FieldType::Boolean
            | FieldType::Decimal
            | FieldType::Timestamp
            | FieldType::Date
            | FieldType::Point
            | FieldType::Duration => result.push(IndexDefinition::SortedInverted(vec![index])),

            // Create sorted inverted and full text indexes for string fields.
            FieldType::String => {
                result.push(IndexDefinition::SortedInverted(vec![index]));
                result.push(IndexDefinition::FullText(index));
            }

            // Skip creating indexes
            FieldType::Text | FieldType::Binary | FieldType::Json => (),
        }
    }

    // Create requested indexes.
    for index in &config.create {
        match index {
            SecondaryIndex::SortedInverted(SortedInverted { fields }) => {
                let fields = fields
                    .iter()
                    .map(|field| field_index_from_field_name(field_definitions, field))
                    .collect::<Result<Vec<_>, _>>()?;
                result.push(IndexDefinition::SortedInverted(fields));
            }
            SecondaryIndex::FullText(FullText { field }) => {
                let field = field_index_from_field_name(field_definitions, field)?;
                result.push(IndexDefinition::FullText(field));
            }
        }
    }

    Ok(result)
}

fn field_index_from_field_name(
    fields: &[FieldDefinition],
    field_name: &str,
) -> Result<usize, BuildError> {
    fields
        .iter()
        .position(|field| field.name == field_name)
        .ok_or(BuildError::FieldNotFound(field_name.to_string()))
}
