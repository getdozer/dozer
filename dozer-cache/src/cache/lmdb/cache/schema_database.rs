use std::{borrow::Cow, collections::HashMap};

use dozer_storage::{lmdb::RwTransaction, lmdb_storage::LmdbEnvironmentManager, LmdbMap};
use dozer_types::types::{IndexDefinition, Schema, SchemaIdentifier};

use crate::errors::CacheError;

#[derive(Debug, Clone)]
pub struct SchemaDatabase {
    database: LmdbMap<str, (Schema, Vec<IndexDefinition>)>,
    schemas: Vec<(Schema, Vec<IndexDefinition>)>,
    schema_name_to_index: HashMap<String, usize>,
    schema_id_to_index: HashMap<SchemaIdentifier, usize>,
}

impl SchemaDatabase {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let database = LmdbMap::new_from_env(env, Some("schemas"), create_if_not_exist)?;

        // Collect existing schemas.
        let txn = env.begin_ro_txn()?;
        let mut schema_name_to_index = HashMap::new();
        let mut schema_id_to_index = HashMap::new();
        let schemas = database
            .iter(&txn)?
            .enumerate()
            .map(|(index, result)| {
                result.map_err(CacheError::Storage).and_then(
                    |(name, schema_and_indexes): (
                        Cow<str>,
                        Cow<(Schema, Vec<IndexDefinition>)>,
                    )| {
                        let (schema, indexes) = schema_and_indexes.into_owned();
                        schema_name_to_index.insert(name.into_owned(), index);
                        let identifier =
                            schema.identifier.ok_or(CacheError::SchemaHasNoIdentifier)?;
                        if schema_id_to_index.insert(identifier, index).is_some() {
                            return Err(CacheError::DuplicateSchemaIdentifier(identifier));
                        }
                        Ok((schema, indexes))
                    },
                )
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            database,
            schemas,
            schema_name_to_index,
            schema_id_to_index,
        })
    }

    pub fn insert(
        &mut self,
        txn: &mut RwTransaction,
        schema_name: String,
        schema: Schema,
        secondary_indexes: Vec<IndexDefinition>,
    ) -> Result<(), CacheError> {
        let identifier = schema.identifier.ok_or(CacheError::SchemaHasNoIdentifier)?;

        let schema_and_indexes = (schema, secondary_indexes);
        if !self
            .database
            .insert(txn, &schema_name, &schema_and_indexes)?
        {
            panic!("Schema {schema_name} already exists");
        }

        let index = self.schemas.len();
        self.schemas.push(schema_and_indexes);
        self.schema_name_to_index.insert(schema_name, index);
        if self.schema_id_to_index.insert(identifier, index).is_some() {
            return Err(CacheError::DuplicateSchemaIdentifier(identifier));
        }

        Ok(())
    }

    pub fn get_schema_from_name(&self, name: &str) -> Option<&(Schema, Vec<IndexDefinition>)> {
        self.schema_name_to_index
            .get(name)
            .map(|index| &self.schemas[*index])
    }

    pub fn get_schema(
        &self,
        identifier: SchemaIdentifier,
    ) -> Option<&(Schema, Vec<IndexDefinition>)> {
        self.schema_id_to_index
            .get(&identifier)
            .map(|index| &self.schemas[*index])
    }

    pub fn get_all_schemas(&self) -> &[(Schema, Vec<IndexDefinition>)] {
        &self.schemas
    }
}

#[cfg(test)]
mod tests {
    use dozer_storage::{errors::StorageError, lmdb::Transaction};
    use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

    use crate::cache::lmdb::utils::{init_env, CacheOptions};

    use super::*;

    fn get_all_schemas<T: Transaction>(
        txn: &T,
        map: LmdbMap<str, (Schema, Vec<IndexDefinition>)>,
    ) -> Result<Vec<(String, Schema, Vec<IndexDefinition>)>, StorageError> {
        map.iter(txn)?
            .map(|result| {
                result.map(
                    |(name, schema_and_indexes): (
                        Cow<str>,
                        Cow<(Schema, Vec<IndexDefinition>)>,
                    )| {
                        let (schema, indexes) = schema_and_indexes.into_owned();
                        (name.into_owned(), schema, indexes)
                    },
                )
            })
            .collect()
    }

    #[test]
    fn test_schema_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap().0;
        let mut writer = SchemaDatabase::new(&mut env, true).unwrap();

        let schema_name = "test_schema";
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
            .insert(
                &mut txn,
                schema_name.to_string(),
                schema.clone(),
                secondary_indexes.clone(),
            )
            .unwrap();
        txn.commit().unwrap();

        let reader = SchemaDatabase::new(&mut env, false).unwrap();
        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();

        let expected = (schema, secondary_indexes);
        assert_eq!(writer.get_schema_from_name(schema_name).unwrap(), &expected);
        assert_eq!(reader.get_schema_from_name(schema_name).unwrap(), &expected);
        assert_eq!(
            writer.get_schema(expected.0.identifier.unwrap()).unwrap(),
            &expected
        );
        assert_eq!(
            reader.get_schema(expected.0.identifier.unwrap()).unwrap(),
            &expected
        );
        txn.commit_and_renew().unwrap();

        let (schema, secondary_indexes) = expected;
        assert_eq!(
            get_all_schemas(txn.txn(), writer.database).unwrap(),
            vec![(
                schema_name.to_string(),
                schema.clone(),
                secondary_indexes.clone()
            )]
        );
        assert_eq!(
            get_all_schemas(txn.txn(), reader.database).unwrap(),
            vec![(schema_name.to_string(), schema, secondary_indexes)]
        );
    }
}
