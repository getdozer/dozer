use std::borrow::Cow;

use dozer_types::{
    models::api_endpoint::{
        ApiEndpoint, FullText, SecondaryIndex, SecondaryIndexConfig, SortedInverted,
    },
    types::{FieldDefinition, FieldType, IndexDefinition, Schema, SchemaWithIndex},
};

use crate::errors::BuildError;

pub fn modify_schema(
    schema: &Schema,
    api_endpoint: &ApiEndpoint,
) -> Result<SchemaWithIndex, BuildError> {
    let mut schema = schema.clone();
    // Generated Cache index based on api_index
    let configured_index = create_primary_indexes(
        &schema.fields,
        api_endpoint
            .index
            .as_ref()
            .map(|index| index.primary_key.as_slice()),
    )?;
    // Generated schema in SQL
    let upstream_index = schema.primary_index.clone();

    let index = match (configured_index.is_empty(), upstream_index.is_empty()) {
        (true, true) => vec![],
        (true, false) => upstream_index,
        (false, true) => configured_index,
        (false, false) => {
            if !upstream_index.eq(&configured_index) {
                return Err(BuildError::MismatchPrimaryKey {
                    endpoint_name: api_endpoint.name.clone(),
                    expected: get_field_names(&schema, &upstream_index),
                    actual: get_field_names(&schema, &configured_index),
                });
            }
            configured_index
        }
    };

    schema.primary_index = index;

    let secondary_index_config = get_secondary_index_config(api_endpoint);
    let secondary_indexes = generate_secondary_indexes(&schema.fields, &secondary_index_config)?;

    Ok((schema, secondary_indexes))
}

fn get_field_names(schema: &Schema, indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|idx| schema.fields[*idx].name.to_owned())
        .collect()
}

fn get_secondary_index_config(api_endpoint: &ApiEndpoint) -> Cow<SecondaryIndexConfig> {
    if let Some(config) = api_endpoint
        .index
        .as_ref()
        .and_then(|index| index.secondary.as_ref())
    {
        Cow::Borrowed(config)
    } else {
        Cow::Owned(SecondaryIndexConfig::default())
    }
}

fn create_primary_indexes(
    field_definitions: &[FieldDefinition],
    primary_key: Option<&[String]>,
) -> Result<Vec<usize>, BuildError> {
    let mut primary_index = Vec::new();
    if let Some(primary_key) = primary_key {
        for name in primary_key {
            primary_index.push(field_index_from_field_name(field_definitions, name)?);
        }
    }
    Ok(primary_index)
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
    for create in &config.create {
        if let Some(index) = &create.index {
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
