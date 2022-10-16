use dozer_types::serde;
use lmdb::{Database, RoTransaction, Transaction};
pub fn get<T>(txn: &RoTransaction, db: Database, key: &[u8]) -> anyhow::Result<T>
where
    T: for<'a> serde::de::Deserialize<'a>,
{
    let rec = txn.get(db, &key)?;
    let rec: T = bincode::deserialize(rec)?;
    Ok(rec)
}
