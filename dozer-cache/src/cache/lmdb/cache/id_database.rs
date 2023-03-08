use dozer_storage::{errors::StorageError, lmdb::RwTransaction, LmdbMap};

pub fn get_or_generate_id(
    map: LmdbMap<Vec<u8>, u64>,
    txn: &mut RwTransaction,
    key: Option<&[u8]>,
) -> Result<u64, StorageError> {
    if let Some(key) = key {
        match map.get(txn, key)? {
            Some(id) => Ok(id.into_owned()),
            None => generate_id(map, txn, Some(key)),
        }
    } else {
        generate_id(map, txn, None)
    }
}

fn generate_id(
    map: LmdbMap<Vec<u8>, u64>,
    txn: &mut RwTransaction,
    key: Option<&[u8]>,
) -> Result<u64, StorageError> {
    let id = map.count(txn)? as u64;

    let id_bytes = id.to_be_bytes();
    let key = key.unwrap_or(&id_bytes);

    if !map.insert(txn, key, &id)? {
        panic!("Generating id for existing key");
    }

    Ok(id)
}

#[cfg(test)]
mod tests {
    use crate::cache::lmdb::utils::{init_env, CacheOptions};

    use super::*;

    #[test]
    fn test_id_database() {
        let mut env = init_env(&CacheOptions::default()).unwrap().0;
        let name = Some("primary_index");
        let writer = LmdbMap::new_from_env(&mut env, name, true).unwrap();
        let reader = LmdbMap::<Vec<u8>, u64>::new_from_env(&mut env, name, false).unwrap();

        let key = b"key";

        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();
        let id = get_or_generate_id(writer, txn.txn_mut(), Some(key)).unwrap();
        get_or_generate_id(writer, txn.txn_mut(), None).unwrap();
        txn.commit_and_renew().unwrap();

        assert_eq!(
            writer.get(txn.txn(), key).unwrap().unwrap().into_owned(),
            id
        );
        assert_eq!(
            reader.get(txn.txn(), key).unwrap().unwrap().into_owned(),
            id
        );
        txn.commit_and_renew().unwrap();
    }
}
