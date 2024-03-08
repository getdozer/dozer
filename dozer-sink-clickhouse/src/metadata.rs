use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};

// Replication Metadata Constants
pub const REPLICA_METADATA_TABLE: &str = "__dozer_replication_metadata";
pub const META_TABLE_COL: &str = "table";
pub const META_TXN_ID_COL: &str = "txn_id";

pub struct ReplicationMetadata {
    pub schema: Schema,
    pub table_name: String,
}

impl ReplicationMetadata {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_primary_keys(&self) -> Vec<String> {
        vec![META_TABLE_COL.to_string()]
    }

    pub fn get_metadata() -> ReplicationMetadata {
        ReplicationMetadata {
            table_name: REPLICA_METADATA_TABLE.to_string(),
            schema: Schema::new()
                .field(
                    FieldDefinition {
                        name: META_TABLE_COL.to_owned(),
                        typ: FieldType::String,
                        nullable: false,
                        source: SourceDefinition::Dynamic,
                    },
                    true,
                )
                .field(
                    FieldDefinition {
                        name: META_TXN_ID_COL.to_owned(),
                        typ: FieldType::UInt,
                        nullable: false,
                        source: SourceDefinition::Dynamic,
                    },
                    false,
                )
                .clone(),
        }
    }
}
