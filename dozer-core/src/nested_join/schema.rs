use crate::dag::dag::PortHandle;
use crate::nested_join::nested_join::{NestedJoinChildConfig, NestedJoinConfig};
use anyhow::{anyhow, Context};
use dozer_types::types::{FieldDefinition, FieldType, Schema};
use std::collections::HashMap;

pub struct NestedJoinIndexParent {
    pub(crate) parent: PortHandle,
    pub(crate) parent_array_index: usize,
    pub(crate) parent_join_key_indexes: Vec<usize>,
    pub(crate) join_key_indexes: Vec<usize>,
}

pub struct NestedJoinIndex {
    pub(crate) id: PortHandle,
    pub(crate) children: Vec<PortHandle>,
    pub(crate) parent: Option<NestedJoinIndexParent>,
}

fn get_nested_schema_index(
    parent_schema_id: PortHandle,
    children: &Vec<NestedJoinChildConfig>,
    input_schemas: &HashMap<PortHandle, Schema>,
    parent_index: &mut HashMap<PortHandle, NestedJoinIndex>,
) -> anyhow::Result<()> {
    for config in children {
        let parent_schema = input_schemas
            .get(&parent_schema_id)
            .context(anyhow!("Unable to find schema id {}", &parent_schema_id))?;
        let parent_array_index = parent_schema.get_field_index(&config.parent_array_field)?.0;

        let mut parent_join_key_indexes = Vec::<usize>::new();
        for name in &config.parent_join_key_fields {
            parent_join_key_indexes.push(parent_schema.get_field_index(name.as_str())?.0);
        }

        let child_schema = input_schemas
            .get(&config.port)
            .context(anyhow!("Unable to find schema id {}", &config.port))?;
        let mut join_key_indexes = Vec::<usize>::new();
        for name in &config.join_key_fields {
            join_key_indexes.push(child_schema.get_field_index(name.as_str())?.0);
        }

        let idx = NestedJoinIndex {
            id: config.port,
            children: config.children.iter().map(|e| e.port).collect(),
            parent: Some(NestedJoinIndexParent {
                parent: parent_schema_id,
                parent_array_index,
                parent_join_key_indexes,
                join_key_indexes,
            }),
        };
        parent_index.insert(config.port, idx);
        get_nested_schema_index(config.port, &config.children, input_schemas, parent_index)?;
    }
    Ok(())
}

pub(crate) fn generate_nested_schema_index(
    config: NestedJoinConfig,
    input_schemas: &HashMap<PortHandle, Schema>,
) -> anyhow::Result<HashMap<PortHandle, NestedJoinIndex>> {
    let mut idx: HashMap<PortHandle, NestedJoinIndex> = HashMap::new();

    idx.insert(
        config.port,
        NestedJoinIndex {
            id: config.port,
            children: config.children.iter().map(|e| e.port).collect(),
            parent: None,
        },
    );
    get_nested_schema_index(config.port, &config.children, input_schemas, &mut idx)?;
    Ok(idx)
}

fn get_nested_schema(
    parent_schema_id: PortHandle,
    children: &Vec<NestedJoinChildConfig>,
    input_schemas: &HashMap<PortHandle, Schema>,
) -> anyhow::Result<Schema> {
    let config_idx_tuples: _ = children.iter().map(|e| (e.parent_array_field.clone(), e));

    let config_idx: HashMap<String, &NestedJoinChildConfig> = HashMap::from_iter(config_idx_tuples);

    let parent_schema = input_schemas
        .get(&parent_schema_id)
        .context(anyhow!("Unable to find parent schema"))?;
    let mut out_schema = Schema::empty();

    for f in parent_schema.fields.iter() {
        match config_idx.get(&f.name) {
            Some(cfg) => {
                let child_schema = get_nested_schema(cfg.port, &cfg.children, input_schemas)?;
                out_schema.fields.push(FieldDefinition::new(
                    f.name.clone(),
                    FieldType::RecordArray(child_schema),
                    false,
                ));
            }
            _ => out_schema.fields.push(f.clone()),
        }
    }

    out_schema.values = parent_schema.values.clone();
    out_schema.primary_index = parent_schema.primary_index.clone();
    Ok(out_schema)
}

pub fn generate_nested_schema(
    input_schemas: &HashMap<PortHandle, Schema>,
    config: NestedJoinConfig,
) -> anyhow::Result<Schema> {
    get_nested_schema(config.port, &config.children, input_schemas)
}
