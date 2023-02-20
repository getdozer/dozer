use std::collections::HashMap;

use dozer_storage::{
    errors::StorageError,
    lmdb::{Cursor, Database, DatabaseFlags, RwTransaction, Transaction, WriteFlags},
    lmdb_storage::LmdbEnvironmentManager,
};
use dozer_types::{
    bincode,
    types::{IndexDefinition, Schema, SchemaIdentifier},
};

use crate::errors::{CacheError, QueryError};

#[derive(Debug, Clone)]
pub struct SchemaDatabase {
    inner: Database,
    schemas: Vec<(Schema, Vec<IndexDefinition>)>,
    schema_name_to_index: HashMap<String, usize>,
    schema_id_to_index: HashMap<SchemaIdentifier, usize>,
}

impl SchemaDatabase {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        // Open or create database.
        let flags = if create_if_not_exist {
            Some(DatabaseFlags::empty())
        } else {
            None
        };
        let db = env.create_database(Some("schemas"), flags)?;

        // Collect existing schemas.
        let txn = env.begin_ro_txn()?;
        let mut schema_name_to_index = HashMap::new();
        let mut schema_id_to_index = HashMap::new();
        let schemas = get_all_schemas(&txn, db)?
            .into_iter()
            .enumerate()
            .map(|(index, (name, schema, indexes))| {
                schema_name_to_index.insert(name.to_string(), index);
                let identifier = schema.identifier.ok_or(CacheError::SchemaHasNoIdentifier)?;
                if schema_id_to_index.insert(identifier, index).is_some() {
                    return Err(CacheError::DuplicateSchemaIdentifier(identifier));
                }
                Ok((schema, indexes))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            inner: db,
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

        let encoded: Vec<u8> = bincode::serialize(&(&schema, &secondary_indexes))
            .map_err(CacheError::map_serialization_error)?;

        txn.put(self.inner, &schema_name, &encoded, WriteFlags::NO_OVERWRITE)
            .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))?;

        let index = self.schemas.len();
        self.schemas.push((schema, secondary_indexes));
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

#[allow(clippy::type_complexity)]
fn get_all_schemas<T: Transaction>(
    txn: &T,
    db: Database,
) -> Result<Vec<(&str, Schema, Vec<IndexDefinition>)>, CacheError> {
    let mut cursor = txn
        .open_ro_cursor(db)
        .map_err(|e| CacheError::Storage(StorageError::InternalDbError(e)))?;

    let mut result = vec![];
    for item in cursor.iter_start() {
        let (key, value) = item.map_err(QueryError::GetValue)?;
        let name = std::str::from_utf8(key).expect("Schema name should always be utf8 string");
        let (schema, indexes): (Schema, Vec<IndexDefinition>) =
            bincode::deserialize(value).map_err(CacheError::map_deserialization_error)?;
        result.push((name, schema, indexes));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

    use crate::cache::lmdb::utils::{init_env, CacheOptions};

    use super::*;

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
            get_all_schemas(txn.txn(), writer.inner).unwrap(),
            vec![(schema_name, schema.clone(), secondary_indexes.clone())]
        );
        assert_eq!(
            get_all_schemas(txn.txn(), reader.inner).unwrap(),
            vec![(schema_name, schema, secondary_indexes)]
        );
    }
}
