use dozer_types::{
    models::endpoint::{FullText, SecondaryIndex, SecondaryIndexConfig, SortedInverted},
    types::{FieldDefinition, FieldType, IndexDefinition, Schema, SchemaWithIndex},
};

use crate::errors::BuildError;

pub fn modify_schema(schema: &Schema) -> Result<SchemaWithIndex, BuildError> {
    let mut schema = schema.clone();

    // Generated schema in SQL
    let upstream_index = schema.primary_index.clone();
    let index = if upstream_index.is_empty() {
        vec![]
    } else {
        upstream_index
    };

    schema.primary_index = index;
    let secondary_index_config = SecondaryIndexConfig::default();
    let secondary_indexes = generate_secondary_indexes(&schema.fields, &secondary_index_config)?;

    Ok((schema, secondary_indexes))
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
