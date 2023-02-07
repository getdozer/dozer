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

#[derive(Debug, Clone, Copy)]
pub struct SchemaDatabase(Database);

impl SchemaDatabase {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let flags = if create_if_not_exist {
            Some(DatabaseFlags::empty())
        } else {
            None
        };
        let db = env.create_database(Some("schemas"), flags)?;
        Ok(Self(db))
    }

    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        schema_name: &str,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        let encoded: Vec<u8> = bincode::serialize(&(schema, secondary_indexes))
            .map_err(CacheError::map_serialization_error)?;
        let schema_id = schema
            .identifier
            .ok_or(CacheError::SchemaIdentifierNotFound)?;
        let key = get_schema_key(schema_id);

        // Insert Schema with {id, version}
        txn.put::<Vec<u8>, Vec<u8>>(self.0, &key, &encoded, WriteFlags::NO_OVERWRITE)
            .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))?;

        let schema_id_bytes =
            bincode::serialize(&schema_id).map_err(CacheError::map_serialization_error)?;

        // Insert Reverse key lookup for schema by name
        let schema_key = get_schema_reverse_key(schema_name);

        txn.put::<Vec<u8>, Vec<u8>>(
            self.0,
            &schema_key,
            &schema_id_bytes,
            WriteFlags::NO_OVERWRITE,
        )
        .map_err(|e| CacheError::Query(QueryError::InsertValue(e)))?;

        Ok(())
    }

    pub fn get_schema_from_name<T: Transaction>(
        &self,
        txn: &T,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let schema_reverse_key = get_schema_reverse_key(name);
        let schema_identifier = txn
            .get(self.0, &schema_reverse_key)
            .map_err(|e| CacheError::Query(QueryError::GetValue(e)))?;
        let schema_id: SchemaIdentifier = bincode::deserialize(schema_identifier)
            .map_err(CacheError::map_deserialization_error)?;

        let schema = self.get_schema(txn, schema_id)?;

        Ok(schema)
    }

    pub fn get_schema<T: Transaction>(
        &self,
        txn: &T,
        identifier: SchemaIdentifier,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let key = get_schema_key(identifier);
        let schema = txn
            .get(self.0, &key)
            .map_err(|e| CacheError::Query(QueryError::GetSchema(e)))?;
        let schema = bincode::deserialize(schema).map_err(CacheError::map_deserialization_error)?;
        Ok(schema)
    }

    pub fn get_all_schemas(
        &self,
        env: &mut LmdbEnvironmentManager,
    ) -> Result<Vec<(Schema, Vec<IndexDefinition>)>, CacheError> {
        let txn = env.begin_ro_txn()?;
        let schemas = get_all_schemas(&txn, self.0)?;
        txn.commit().map_err(StorageError::InternalDbError)?;
        Ok(schemas)
    }
}

fn get_schema_key(schema_id: SchemaIdentifier) -> Vec<u8> {
    [
        "sc".as_bytes(),
        schema_id.id.to_be_bytes().as_ref(),
        schema_id.version.to_be_bytes().as_ref(),
    ]
    .join("#".as_bytes())
}

fn get_all_schemas<T: Transaction>(
    txn: &T,
    db: Database,
) -> Result<Vec<(Schema, Vec<IndexDefinition>)>, CacheError> {
    let mut cursor = txn
        .open_ro_cursor(db)
        .map_err(|e| CacheError::Storage(StorageError::InternalDbError(e)))?;

    let mut result = vec![];
    for item in cursor.iter_start() {
        let (key, value) = item.map_err(QueryError::GetValue)?;
        if key.starts_with(b"sc#") {
            let schema: (Schema, Vec<IndexDefinition>) =
                bincode::deserialize(value).map_err(CacheError::map_deserialization_error)?;
            result.push(schema);
        }
    }
    Ok(result)
}
const SCHEMA_NAME_PREFIX: &str = "schema_name_";

fn get_schema_reverse_key(name: &str) -> Vec<u8> {
    format!("{SCHEMA_NAME_PREFIX}{name}").into_bytes()
}

#[cfg(test)]
mod tests {
    use dozer_storage::lmdb_storage::LmdbTransaction;
    use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

    use crate::cache::{lmdb::utils::init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_schema_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap();
        let writer = SchemaDatabase::new(&mut env, true).unwrap();
        let reader = SchemaDatabase::new(&mut env, false).unwrap();

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

        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();
        writer
            .insert(txn.txn_mut(), schema_name, &schema, &secondary_indexes)
            .unwrap();
        txn.commit_and_renew().unwrap();

        assert_eq!(
            writer.get_schema_from_name(txn.txn(), schema_name).unwrap(),
            (schema.clone(), secondary_indexes.clone())
        );
        assert_eq!(
            reader.get_schema_from_name(txn.txn(), schema_name).unwrap(),
            (schema.clone(), secondary_indexes.clone())
        );
        assert_eq!(
            writer
                .get_schema(txn.txn(), schema.identifier.unwrap())
                .unwrap(),
            (schema.clone(), secondary_indexes.clone())
        );
        assert_eq!(
            reader
                .get_schema(txn.txn(), schema.identifier.unwrap())
                .unwrap(),
            (schema.clone(), secondary_indexes.clone())
        );
        txn.commit_and_renew().unwrap();

        assert_eq!(
            get_all_schemas(txn.txn(), writer.0).unwrap(),
            vec![(schema.clone(), secondary_indexes.clone())]
        );
        assert_eq!(
            get_all_schemas(txn.txn(), reader.0).unwrap(),
            vec![(schema, secondary_indexes)]
        );
    }
}
