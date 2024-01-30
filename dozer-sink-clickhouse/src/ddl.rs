use dozer_types::log::warn;
use dozer_types::models::endpoint::ClickhouseSinkTableOptions;
use dozer_types::types::{FieldDefinition, FieldType, Schema};

pub struct ClickhouseDDL {}

const DEFAULT_TABLE_ENGINE: &str = "MergeTree()";

impl ClickhouseDDL {
    pub fn get_create_table_query(
        table_name: String,
        schema: Schema,
        sink_options: Option<ClickhouseSinkTableOptions>,
        primary_keys: Option<Vec<String>>,
    ) -> String {
        let mut parts = schema
            .fields
            .iter()
            .map(|field| {
                let typ = Self::map_field_to_type(field);
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

    pub fn map_field_to_type(field: &FieldDefinition) -> String {
        let typ = match field.typ {
            FieldType::UInt => "UInt64",
            FieldType::U128 => "UInt128",
            FieldType::Int => "Int64",
            FieldType::I128 => "Int128",
            FieldType::Float => "Float64",
            FieldType::Boolean => "Boolean",
            FieldType::String => "String",
            FieldType::Text => "String",
            FieldType::Binary => "Array(UInt8)",
            FieldType::Decimal => "Decimal",
            FieldType::Timestamp => "DateTime64(3)",
            FieldType::Date => "Date",
            FieldType::Json => "JSON",
            FieldType::Point => "Point",
            FieldType::Duration => unimplemented!(),
        };

        if field.nullable {
            if field.typ != FieldType::Binary {
                format!("Nullable({})", typ)
            } else {
                warn!("Binary field cannot be nullable, ignoring nullable flag");
                typ.to_string()
            }
        } else {
            typ.to_string()
        }
    }
}
