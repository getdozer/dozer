use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::SerializationError;
use crate::storage::lmdb_sys::{Database, LmdbError, PutOptions, Transaction};
use std::mem;

pub struct SecondaryIndexKey<'a> {
    index_id: u16,
    value: &'a [u8],
}

impl<'a> SecondaryIndexKey<'a> {
    pub fn new(index_id: u16, value: &'a [u8]) -> Self {
        Self { index_id, value }
    }
}

pub struct IndexedDatabase {
    db: Database,
    counter_key: [u8; 2],
}

const COUNTER_KEY: u16 = 0_u16;
const PRIMARY_IDX_KEY: u16 = 1_u16;
const SECONDARY_IDX_KEY_LIST_KEY: u16 = 2_u16;

impl IndexedDatabase {
    pub fn new(db: Database) -> Self {
        let mut counter_key: [u8; 2] = [0; 2];
        counter_key[..2].copy_from_slice(&COUNTER_KEY.to_be_bytes());

        Self { db, counter_key }
    }

    fn create_key(&self, index_id: u16, data: &[u8]) -> Vec<u8> {
        let mut full_key_vec = Vec::with_capacity(data.len() + mem::size_of_val(&index_id));
        full_key_vec.extend_from_slice(&index_id.to_be_bytes());
        full_key_vec.extend(data);
        full_key_vec
    }

    fn get_autogen_counter(&self, tx: &mut Transaction) -> Result<u64, StorageError> {
        let curr_counter = match tx.get(&self.db, &self.counter_key)? {
            Some(c) => u64::from_be_bytes(c.try_into().map_err(|e| {
                StorageError::DeserializationError {
                    typ: "u64".to_string(),
                    reason: Box::new(e),
                }
            })?),
            _ => 1_u64,
        };
        tx.put(
            &self.db,
            &self.counter_key,
            &(curr_counter + 1).to_be_bytes(),
            PutOptions::default(),
        )?;
        Ok(curr_counter)
    }

    pub fn del(&mut self, tx: &mut Transaction, key: &[u8]) -> Result<(), StorageError> {
        let sec_key_list_key = self.create_key(SECONDARY_IDX_KEY_LIST_KEY, key);
        let full_pk = self.create_key(PRIMARY_IDX_KEY, key);

        if let Some(buf) = tx.get(&self.db, sec_key_list_key.as_slice())? {
            let sec_keys: Vec<Vec<u8>> =
                bincode::deserialize(buf).map_err(|e| StorageError::DeserializationError {
                    typ: "Vec<Vec<u8>>".to_string(),
                    reason: Box::new(e),
                })?;
            for key in &sec_keys {
                tx.del(&self.db, key, Some(full_pk.as_slice()))?;
            }
        }
        tx.del(&self.db, full_pk.as_slice(), None)?;
        Ok(())
    }

    pub fn put(
        &mut self,
        tx: &mut Transaction,
        key: Option<&[u8]>,
        value: &[u8],
        indexes: Vec<SecondaryIndexKey>,
    ) -> Result<Vec<u8>, StorageError> {
        let pk_val = match key {
            Some(v) => {
                self.del(tx, v)?;
                Vec::from(v)
            }
            _ => {
                let ctr = self.get_autogen_counter(tx)?.to_be_bytes();
                Vec::from(ctr.as_slice())
            }
        };

        let full_pk = self.create_key(PRIMARY_IDX_KEY, pk_val.as_slice());
        tx.put(&self.db, full_pk.as_slice(), value, PutOptions::default())?;

        let full_secondary_keys: Vec<Vec<u8>> = indexes
            .iter()
            .map(|e| self.create_key(e.index_id, e.value))
            .collect();

        for fk in &full_secondary_keys {
            tx.put(
                &self.db,
                fk.as_slice(),
                full_pk.as_slice(),
                PutOptions::default(),
            )?;
        }

        let sec_key_list_key = self.create_key(SECONDARY_IDX_KEY_LIST_KEY, pk_val.as_slice());
        let sec_key_list_val =
            bincode::serialize(&full_secondary_keys).map_err(|e| SerializationError {
                typ: "Vec<Vec<u8>>".to_string(),
                reason: Box::new(e),
            })?;
        tx.put(
            &self.db,
            sec_key_list_key.as_slice(),
            sec_key_list_val.as_slice(),
            PutOptions::default(),
        )?;
        Ok(full_pk)
    }

    pub fn get<'a>(
        &self,
        tx: &'a Transaction,
        pk_val: &[u8],
    ) -> Result<Option<&'a [u8]>, LmdbError> {
        let full_pk = self.create_key(PRIMARY_IDX_KEY, pk_val);
        tx.get(&self.db, full_pk.as_slice())
    }

    pub fn get_multi<'a>(
        &self,
        tx: &'a mut Transaction,
        index_id: u16,
        key: &[u8],
    ) -> Result<Vec<&'a [u8]>, StorageError> {
        let full_key = self.create_key(index_id, key);
        let mut records = Vec::<&[u8]>::new();
        let cursor = tx.open_cursor(&self.db)?;
        if cursor.seek(full_key.as_slice())? {
            loop {
                let kv = cursor.read()?;
                match kv {
                    Some(t) => {
                        if t.0 != full_key {
                            break;
                        }
                        match tx.get(&self.db, t.1)? {
                            Some(v) => records.push(v),
                            _ => return Err(StorageError::InvalidKey(format!("{:x?}", t.1))),
                        }
                    }
                    _ => break,
                }
                if !cursor.next()? {
                    break;
                }
            }
        }
        Ok(records)
    }
}
