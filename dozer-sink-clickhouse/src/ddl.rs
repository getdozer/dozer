use crate::client::AggregationFunction;
use dozer_types::models::sink::ClickhouseTableOptions;
use dozer_types::types::FieldDefinition;
use std::collections::HashMap;

use crate::schema::map_field_to_type;

const DEFAULT_TABLE_ENGINE: &str = "MergeTree()";

pub fn get_create_table_query(
    table_name: &str,
    fields: &[FieldDefinition],
    table_options: Option<ClickhouseTableOptions>,
    aggregations: HashMap<String, AggregationFunction>,
) -> String {
    let mut parts = fields
        .iter()
        .map(|field| {
            let typ = map_field_to_type(field);

            match aggregations.get(&field.name) {
                Some(agg) => format!(
                    "`{}` AggregateFunction({}, {})",
                    field.name, agg.aggregation, typ
                ),
                None => format!("`{}` {}", field.name, typ),
            }
        })
        .collect::<Vec<_>>();

    let engine = table_options
        .as_ref()
        .and_then(|c| c.engine.clone())
        .unwrap_or_else(|| DEFAULT_TABLE_ENGINE.to_string());

    parts.push(
        table_options
            .as_ref()
            .and_then(|options| options.primary_keys.clone())
            .map_or("".to_string(), |pk| {
                format!("PRIMARY KEY (`{}`)", pk.join(", "))
            }),
    );

    let query = parts.join(",\n");

    let partition_by = table_options
        .as_ref()
        .and_then(|options| options.partition_by.clone())
        .map_or("".to_string(), |partition_by| {
            format!("PARTITION BY `{}`\n", partition_by)
        });
    let sample_by = table_options
        .as_ref()
        .and_then(|options| options.sample_by.clone())
        .map_or("".to_string(), |partition_by| {
            format!("SAMPLE BY `{}`\n", partition_by)
        });
    let order_by = table_options
        .as_ref()
        .and_then(|options| options.order_by.clone())
        .map_or("".to_string(), |order_by| {
            format!("ORDER BY (`{}`)\n", order_by.join(", "))
        });
    let cluster = table_options
        .as_ref()
        .and_then(|options| options.cluster.clone())
        .map_or("".to_string(), |cluster| {
            format!("ON CLUSTER `{}`\n", cluster)
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
