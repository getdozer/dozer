use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::types::FieldDefinition;

use crate::schema::map_field_to_type;

const DEFAULT_TABLE_ENGINE: &str = "MergeTree()";

pub fn get_create_table_query(
    table_name: &str,
    fields: &[FieldDefinition],
    config: ClickhouseSinkConfig,
) -> String {
    let mut parts = fields
        .iter()
        .map(|field| {
            let typ = map_field_to_type(field);
            format!("{} {}", field.name, typ)
        })
        .collect::<Vec<_>>();

    let engine = config
        .create_table_options
        .as_ref()
        .and_then(|c| c.engine.clone())
        .unwrap_or_else(|| DEFAULT_TABLE_ENGINE.to_string());

    if let Some(pk) = config.primary_keys {
        parts.push(format!("PRIMARY KEY ({})", pk.join(", ")));
    }

    let query = parts.join(",\n");

    let partition_by = config
        .create_table_options
        .as_ref()
        .and_then(|options| options.partition_by.clone())
        .map_or("".to_string(), |partition_by| {
            format!("PARTITION BY {}\n", partition_by)
        });
    let sample_by = config
        .create_table_options
        .as_ref()
        .and_then(|options| options.sample_by.clone())
        .map_or("".to_string(), |partition_by| {
            format!("SAMPLE BY {}\n", partition_by)
        });
    let order_by = config
        .create_table_options
        .as_ref()
        .and_then(|options| options.order_by.clone())
        .map_or("".to_string(), |order_by| {
            format!("ORDER BY ({})\n", order_by.join(", "))
        });
    let cluster = config
        .create_table_options
        .as_ref()
        .and_then(|options| options.cluster.clone())
        .map_or("".to_string(), |cluster| {
            format!("ON CLUSTER {}\n", cluster)
        });

    format!(
        "CREATE TABLE IF NOT EXISTS {table_name} {cluster} (
               {query}
            )
            ENGINE = {engine}
            {order_by}
            {partition_by}
            {sample_by}
            ",
    )
}
