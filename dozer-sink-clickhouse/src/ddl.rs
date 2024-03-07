use dozer_types::models::sink::ClickhouseSinkTableOptions;
use dozer_types::types::FieldDefinition;

use crate::schema::map_field_to_type;

const DEFAULT_TABLE_ENGINE: &str = "MergeTree()";

pub fn get_create_table_query(
    table_name: &str,
    fields: &[FieldDefinition],
    sink_options: Option<ClickhouseSinkTableOptions>,
    primary_keys: Option<Vec<String>>,
) -> String {
    let mut parts = fields
        .iter()
        .map(|field| {
            let typ = map_field_to_type(field);
            format!("{} {}", field.name, typ)
        })
        .collect::<Vec<_>>();

    let engine = sink_options
        .as_ref()
        .map(|options| {
            options
                .engine
                .clone()
                .unwrap_or_else(|| DEFAULT_TABLE_ENGINE.to_string())
        })
        .unwrap_or_else(|| DEFAULT_TABLE_ENGINE.to_string());

    if let Some(pk) = primary_keys {
        parts.push(format!("PRIMARY KEY ({})", pk.join(", ")));
    }

    let query = parts.join(",\n");

    let partition_by = sink_options
        .as_ref()
        .and_then(|options| options.partition_by.clone())
        .map_or("".to_string(), |partition_by| {
            format!("PARTITION BY {}\n", partition_by)
        });
    let sample_by = sink_options
        .as_ref()
        .and_then(|options| options.sample_by.clone())
        .map_or("".to_string(), |partition_by| {
            format!("SAMPLE BY {}\n", partition_by)
        });
    let order_by = sink_options
        .as_ref()
        .and_then(|options| options.order_by.clone())
        .map_or("".to_string(), |order_by| {
            format!("ORDER BY ({})\n", order_by.join(", "))
        });
    let cluster = sink_options
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
