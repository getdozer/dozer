use std::sync::Arc;

use anyhow::{bail, Context};
use lmdb::{
    Cursor, Database, Environment, RoCursor, RoTransaction, RwTransaction, Transaction, WriteFlags,
};
use log::debug;

use dozer_schema::registry::context::Context as SchemaContext;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_schema::storage::get_schema_key;
use dozer_types::types::Record;
use dozer_types::types::{Schema, SchemaIdentifier};

use super::super::Cache;
use super::indexer::Indexer;
use super::query::handler::LmdbQueryHandler;
use super::query::helper;
use super::utils;
use crate::cache::expression::QueryExpression;
use crate::cache::index;

pub struct LmdbCache {
    env: Environment,
    db: Database,
    indexer_db: Database,
    schema_db: Database,
}

fn _debug_dump(cursor: RoCursor) -> anyhow::Result<()> {
    while let Ok((key, val)) = cursor.get(None, None, 8) {
        debug!("key: {:?}, val: {:?}", key.unwrap(), val);
    }
    Ok(())
}
async fn _get_schema_from_registry(
    client: Arc<SchemaRegistryClient>,
    schema_identifier: SchemaIdentifier,
) -> anyhow::Result<Schema> {
    let ctx = SchemaContext::current();
    let schema = client.get(ctx, schema_identifier).await?;
    Ok(schema)
}

/// Dozer Cache
///
/// Insert, update and delete `Schema`.
///
/// # Example
/// ```
/// use dozer_types::types::{Field, Record, Schema, SchemaIdentifier, FieldDefinition};
/// use dozer_cache::cache::{
///     expression::{self, FilterExpression, QueryExpression},
///     index, Cache,
///     LmdbCache
/// };
/// use anyhow::{Context};
/// let cache = LmdbCache::new(true);
/// let schema = Schema {
///    identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
///    fields: vec![FieldDefinition {
///        name: "foo".to_string(),
///        typ: dozer_types::types::FieldType::String,
///        nullable: true,
///    }],
///    values: vec![0],
///    primary_index: vec![0],
///    secondary_indexes: vec![],
/// };
///
/// cache.insert_schema("test", &schema)?;
///
/// let get_schema = cache.get_schema(
///     &schema
///         .identifier
///         .to_owned()
///         .context("schema_id is expected")?,
/// )?;
/// assert_eq!(get_schema, schema, "must be equal");
/// Ok::<(), anyhow::Error>(())
/// ```
///
/// Insert, update and delete records based on a schema. Secondary indexes are automatically created.
///
/// # Example
/// ```
/// use dozer_types::types::{Field, Record, Schema, SchemaIdentifier, FieldDefinition};
/// use dozer_cache::cache::{
///     expression::{self, FilterExpression, QueryExpression},
///     index, Cache,
///     LmdbCache,
///     test_utils
/// };
/// # use anyhow::{bail, Context};
/// # let val = "bar".to_string();
/// # let (cache, schema) = test_utils::setup();
/// # cache.insert_schema("docs", &schema)?;
///
/// let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);
/// cache.insert(&record)?;
///
/// # let key = index::get_primary_key(&[0], &[Field::String(val)]);
///
/// let get_record = cache.get(&key)?;
/// # assert_eq!(get_record, record, "must be equal");
///
/// cache.delete(&key)?;
///
/// cache.get(&key).expect_err("No record found");
/// # Ok::<(), anyhow::Error>(())
/// ```
/// /// Query based on simple and nested expressions.
/// # Example
/// ```
/// # use dozer_types::types::{Field, Record, Schema, SchemaIdentifier, FieldDefinition};
/// # use dozer_cache::cache::{
/// #     expression::{self, FilterExpression, QueryExpression},
/// #     index, Cache,
/// #     LmdbCache,
/// #     test_utils
/// # };
/// # use anyhow::{bail, Context};
/// # let cache = LmdbCache::new(true);
/// # let schema = test_utils::schema_1();
/// # let record = Record::new(
/// #     schema.identifier.clone(),
/// #     vec![
/// #         Field::Int(1),
/// #         Field::String("test".to_string()),
/// #         Field::Int(2),
/// #     ],
/// # );
/// # cache.insert_schema("sample", &schema)?;
/// # cache.insert(&record)?;
///
/// // Query without an expression
/// let query = QueryExpression::new(None, vec![], 10, 0);
/// let records = cache.query("sample", &query)?;
/// # assert_eq!(records.len(), 1, "must be equal");
///
/// let filter = FilterExpression::And(
///     Box::new(FilterExpression::Simple(
///         "a".to_string(),
///         expression::Operator::EQ,
///         Field::Int(1),
///     )),
///     Box::new(FilterExpression::Simple(
///         "b".to_string(),
///         expression::Operator::EQ,
///         Field::String("test".to_string()),
///     )),
/// );
/// // Query with an expression
/// let query = QueryExpression::new(Some(filter), vec![], 10, 0);
/// let records = cache.query("sample", &query)?;
///
/// # assert_eq!(records.len(), 1, "must be equal");
/// # assert_eq!(records[0], record, "must be equal");
///
/// # Ok::<(), anyhow::Error>(())
impl Cache for LmdbCache {
    /// Insert Schema
    fn insert_schema(&self, name: &str, schema: &Schema) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        self._insert_schema(&mut txn, schema, name)?;
        txn.commit()?;
        Ok(())
    }

    /// Get Schema By Name
    fn get_schema_by_name(&self, name: &str) -> anyhow::Result<Schema> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;
        Ok(schema)
    }

    /// Get Schema By Identifier
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> anyhow::Result<Schema> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        self._get_schema(&txn, schema_identifier)
    }

    fn insert(&self, rec: &Record) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        let schema_identifier = match rec.schema_id.to_owned() {
            Some(id) => id,
            None => bail!("cache::Insert - Schema Id is not present"),
        };
        let schema = match self.get_schema(&schema_identifier) {
            Ok(schema) => schema,
            Err(_) => {
                bail!("schema not present");
            }
        };

        self._insert(&mut txn, rec, &schema)?;
        txn.commit()?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;
        txn.commit()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Record> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let rec: Record = helper::get(&txn, self.db, key)?;
        Ok(rec)
    }

    fn query(&self, name: &str, query: &QueryExpression) -> anyhow::Result<Vec<Record>> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;

        let handler = LmdbQueryHandler::new(self.db, self.indexer_db, &txn);
        let records = handler.query(&schema, query)?;
        Ok(records)
    }

    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;

        self._insert(&mut txn, rec, schema)?;
        txn.commit()?;
        Ok(())
    }
}

impl LmdbCache {
    pub fn new(temp_storage: bool) -> Self {
        let env = utils::init_env(temp_storage).unwrap();
        let db = utils::init_db(&env, Some("records")).unwrap();
        let indexer_db = utils::init_db(&env, Some("indexes")).unwrap();
        let schema_db = utils::init_db(&env, Some("schemas")).unwrap();
        Self {
            env,
            db,
            indexer_db,
            schema_db,
        }
    }

    fn _insert(
        &self,
        txn: &mut RwTransaction,
        rec: &Record,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        let p_key = &schema.primary_index;
        let values = &rec.values;
        let key = index::get_primary_key(p_key, values);
        let encoded: Vec<u8> = bincode::serialize(&rec).unwrap();

        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())?;

        let indexer = Indexer::new(self.indexer_db);

        indexer.build_indexes(txn, rec, schema, key)?;

        Ok(())
    }

    fn _insert_schema(
        &self,
        txn: &mut RwTransaction,
        schema: &Schema,
        name: &str,
    ) -> anyhow::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(&schema)?;
        let schema_id = schema.to_owned().identifier.context("schema_id expected")?;
        let key = get_schema_key(&schema_id);
        txn.put::<Vec<u8>, Vec<u8>>(self.schema_db, &key, &encoded, WriteFlags::default())?;

        let schema_bytes = bincode::serialize(&schema_id)?;
        let schema_key = index::get_schema_reverse_key(name);

        txn.put::<Vec<u8>, Vec<u8>>(
            self.schema_db,
            &schema_key,
            &schema_bytes,
            WriteFlags::default(),
        )?;

        Ok(())
    }

    pub fn _debug_dump(&self) -> anyhow::Result<()> {
        let txn: RoTransaction = self.env.begin_ro_txn().unwrap();

        debug!("Records:");
        _debug_dump(txn.open_ro_cursor(self.db)?)?;

        debug!("Indexes:");
        _debug_dump(txn.open_ro_cursor(self.indexer_db)?)?;

        debug!("Schemas:");
        _debug_dump(txn.open_ro_cursor(self.schema_db)?)?;
        Ok(())
    }

    fn _get_schema_from_reverse_key(
        &self,
        name: &str,
        txn: &RoTransaction,
    ) -> anyhow::Result<Schema> {
        let schema_reverse_key = index::get_schema_reverse_key(name);
        let schema_identifier = txn.get(self.schema_db, &schema_reverse_key)?;
        let schema_id: SchemaIdentifier = bincode::deserialize(schema_identifier)?;
        let schema = self._get_schema(txn, &schema_id)?;

        Ok(schema)
    }

    fn _get_schema(
        &self,
        txn: &RoTransaction,
        schema_identifier: &SchemaIdentifier,
    ) -> anyhow::Result<Schema> {
        let key = get_schema_key(schema_identifier);
        let schema = txn.get(self.schema_db, &key)?;
        let schema: Schema = bincode::deserialize(schema)?;
        Ok(schema)
    }
}
