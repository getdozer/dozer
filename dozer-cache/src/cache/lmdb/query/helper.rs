use dozer_types::{
    bincode,
    errors::cache::{CacheError, QueryError},
    serde,
};
use lmdb::{Database, RoTransaction, Transaction};
pub fn get<T>(txn: &RoTransaction, db: Database, key: &[u8]) -> Result<T, CacheError>
where
    T: for<'a> serde::de::Deserialize<'a>,
{
    let rec = txn
        .get(db, &key)
        .map_err(|_e| CacheError::QueryError(QueryError::GetValue))?;
    bincode::deserialize(rec).map_err(CacheError::map_deserialization_error)
}
