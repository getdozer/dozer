use dozer_types::{
    models::sink::ClickhouseTableOptions,
    node::OpIdentifier,
    types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition},
};

use crate::{client::ClickhouseClient, errors::QueryError};

// Replication Metadata Constants
const REPLICA_METADATA_TABLE: &str = "__dozer_replication_metadata";
const TABLE_COL: &str = "table";
const SOURCE_STATE_COL: &str = "source_state";
const OP_ID_COL: &str = "txn_id";

fn schema() -> Schema {
    let mut schema = Schema::new();
    schema
        .field(
            FieldDefinition {
                name: TABLE_COL.to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            true,
        )
        .field(
            FieldDefinition {
                name: SOURCE_STATE_COL.to_string(),
                typ: FieldType::Binary,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            false,
        )
        .field(
            FieldDefinition {
                name: OP_ID_COL.to_string(),
                typ: FieldType::U128,
                nullable: true,
                source: SourceDefinition::Dynamic,
            },
            false,
        );
    schema
}

pub struct ReplicationMetadata {
    schema: Schema,
}

impl ReplicationMetadata {
    pub async fn new(
        client: &ClickhouseClient,
        cluster: Option<String>,
    ) -> Result<ReplicationMetadata, QueryError> {
        let schema = schema();

        let primary_keys = vec![TABLE_COL.to_string()];
        let create_table_options = ClickhouseTableOptions {
            engine: Some("ReplacingMergeTree".to_string()),
            primary_keys: Some(primary_keys.clone()),
            partition_by: Some(TABLE_COL.to_string()),
            // Replaced using this key
            order_by: Some(primary_keys),
            cluster,
            sample_by: None,
        };
        client
            .create_table(
                REPLICA_METADATA_TABLE,
                &schema.fields,
                Some(create_table_options),
                None,
            )
            .await?;

        Ok(ReplicationMetadata { schema })
    }

    pub async fn set_op_id(
        &self,
        client: &ClickhouseClient,
        table: String,
        op_id: OpIdentifier,
    ) -> Result<(), QueryError> {
        let fields = vec![
            Field::String(table),
            Field::Null,
            Field::U128(op_id_to_u128(op_id)),
        ];
        client
            .insert(REPLICA_METADATA_TABLE, &self.schema.fields, fields, None)
            .await
    }

    pub async fn get_op_id(
        &self,
        client: &ClickhouseClient,
        table: &str,
    ) -> Result<Option<OpIdentifier>, QueryError> {
        let query = format!(
            r#"SELECT "{}" FROM "{}" WHERE "{}" = "{}""#,
            OP_ID_COL, REPLICA_METADATA_TABLE, TABLE_COL, table
        );
        let mut client = client.get_client_handle().await?;
        let rows = client.query(query).fetch_all().await?;
        let Some(row) = rows.rows().next() else {
            return Ok(None);
        };
        let op_id = row.get::<u128, _>(OP_ID_COL)?;
        Ok(Some(u128_to_op_id(op_id)))
    }

    pub async fn set_source_state(
        &self,
        client: &ClickhouseClient,
        table: String,
        source_state: Vec<u8>,
    ) -> Result<(), QueryError> {
        let fields = vec![
            Field::String(table),
            Field::Binary(source_state),
            Field::Null,
        ];
        client
            .insert(REPLICA_METADATA_TABLE, &self.schema.fields, fields, None)
            .await
    }

    pub async fn get_source_state(
        &self,
        client: &ClickhouseClient,
        table: &str,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        let query = format!(
            r#"SELECT "{}" FROM "{}" WHERE "{}" = "{}""#,
            SOURCE_STATE_COL, REPLICA_METADATA_TABLE, TABLE_COL, table
        );
        let mut client = client.get_client_handle().await?;
        let rows = client.query(query).fetch_all().await?;
        let Some(row) = rows.rows().next() else {
            return Ok(None);
        };
        Ok(Some(row.get::<Vec<u8>, _>(SOURCE_STATE_COL)?))
    }
}

fn op_id_to_u128(op_id: OpIdentifier) -> u128 {
    (op_id.txid as u128) << 64 | op_id.seq_in_tx as u128
}

fn u128_to_op_id(u128: u128) -> OpIdentifier {
    OpIdentifier {
        txid: (u128 >> 64) as u64,
        seq_in_tx: (u128 & 0xFFFF_FFFF_FFFF_FFFF) as u64,
    }
}
