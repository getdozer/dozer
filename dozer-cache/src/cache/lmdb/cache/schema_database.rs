use dozer_storage::{lmdb::RwTransaction, lmdb_storage::LmdbEnvironmentManager, LmdbMap};
use dozer_types::{
    borrow::IntoOwned,
    types::{IndexDefinition, Schema},
};

use crate::errors::CacheError;

#[derive(Debug, Clone)]
pub struct SchemaDatabase {
    database: LmdbMap<u8, (Schema, Vec<IndexDefinition>)>,
    schema: Option<(Schema, Vec<IndexDefinition>)>,
}

impl SchemaDatabase {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let database = LmdbMap::new_from_env(env, Some("schemas"), create_if_not_exist)?;

        // Collect existing schemas.
        let txn = env.begin_ro_txn()?;
        assert!(database.count(&txn)? <= 1, "More than one schema found");
        let schema = database.get(&txn, &SCHEMA_KEY)?.map(IntoOwned::into_owned);

        Ok(Self { database, schema })
    }

    pub fn insert(
        &mut self,
        txn: &mut RwTransaction,
        schema: Schema,
        secondary_indexes: Vec<IndexDefinition>,
    ) -> Result<(), CacheError> {
        let schema_and_indexes = (schema, secondary_indexes);
        if !self
            .database
            .insert(txn, &SCHEMA_KEY, &schema_and_indexes)?
        {
            panic!("Schema already exists");
        }

        self.schema = Some(schema_and_indexes);

        Ok(())
    }

    pub fn get_schema(&self) -> Option<&(Schema, Vec<IndexDefinition>)> {
        self.schema.as_ref()
    }
}

const SCHEMA_KEY: u8 = 0;

#[cfg(test)]
mod tests {
    use dozer_storage::lmdb::Transaction;
    use dozer_types::types::{FieldDefinition, FieldType, SchemaIdentifier, SourceDefinition};

    use crate::cache::lmdb::utils::{init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_schema_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap().0;
        let mut writer = SchemaDatabase::new(&mut env, true).unwrap();

        let schema = Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![FieldDefinition {
                name: "id".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            }],
            primary_index: vec![0],
        };
        let secondary_indexes = vec![IndexDefinition::SortedInverted(vec![0])];

        let mut txn = env.begin_rw_txn().unwrap();
        writer
            .insert(&mut txn, schema.clone(), secondary_indexes.clone())
            .unwrap();
        txn.commit().unwrap();

        let reader = SchemaDatabase::new(&mut env, false).unwrap();

        let expected = (schema, secondary_indexes);
        assert_eq!(writer.get_schema().unwrap(), &expected);
        assert_eq!(reader.get_schema().unwrap(), &expected);
    }
}
