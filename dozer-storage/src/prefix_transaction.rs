use lmdb::RoCursor;

use crate::common::Database;
use crate::errors::StorageError;

use super::common::Seek;
use super::lmdb_storage::LmdbExclusiveTransaction;

pub struct PrefixTransaction<'a> {
    prefix: [u8; 4],
    tx: &'a mut LmdbExclusiveTransaction,
}

impl<'a> PrefixTransaction<'a> {
    pub fn new(tx: &'a mut LmdbExclusiveTransaction, prefix: u32) -> Self {
        Self {
            tx,
            prefix: prefix.to_be_bytes(),
        }
    }
}

impl<'a> PrefixTransaction<'a> {
    #[inline]
    pub fn get(&self, db: Database, key: &[u8]) -> Result<Option<&[u8]>, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.tx.get(db, &full_key)
    }

    #[inline]
    pub fn put(&mut self, db: Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.tx.put(db, &full_key, value)
    }

    #[inline]
    pub fn del(
        &mut self,
        db: Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.tx.del(db, &full_key, value)
    }

    pub fn open_cursor(&self, db: Database) -> Result<PrefixReaderCursor, StorageError> {
        let cursor = self.tx.open_ro_cursor(db)?;
        Ok(PrefixReaderCursor::new(cursor, self.prefix))
    }
}

pub struct PrefixReaderCursor<'txn> {
    prefix: [u8; 4],
    inner: RoCursor<'txn>,
}

impl<'txn> PrefixReaderCursor<'txn> {
    pub fn new(inner: RoCursor<'txn>, prefix: [u8; 4]) -> Self {
        Self { inner, prefix }
    }
}

impl<'txn> PrefixReaderCursor<'txn> {
    #[inline]
    pub fn seek_gte(&self, key: &[u8]) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.inner.seek_gte(&full_key)
    }

    #[inline]
    pub fn seek(&self, key: &[u8]) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.inner.seek(&full_key)
    }

    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn read(&self) -> Result<Option<(&[u8], &[u8])>, StorageError> {
        match self.inner.read()? {
            Some((k, v)) => Ok(Some((&k[self.prefix.len()..], v))),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn next(&self) -> Result<bool, StorageError> {
        if !self.inner.next()? {
            return Ok(false);
        }
        match self.inner.read()? {
            Some((key, _val)) => Ok(key[0..4] == self.prefix),
            None => Ok(false),
        }
    }

    #[inline]
    pub fn prev(&self) -> Result<bool, StorageError> {
        if !self.inner.prev()? {
            return Ok(false);
        }
        match self.inner.read()? {
            Some((key, _val)) => Ok(key[0..4] == self.prefix),
            None => Ok(false),
        }
    }

    #[inline]
    pub fn first(&self) -> Result<bool, StorageError> {
        self.inner.seek_gte(&self.prefix)
    }

    #[inline]
    pub fn last(&self) -> Result<bool, StorageError> {
        let mut next_prefix = self.prefix;
        next_prefix[self.prefix.len() - 1] += 1;

        if !self.inner.seek_gte(&next_prefix)? {
            if !self.inner.last()? {
                Ok(false)
            } else if let Some(r) = self.inner.read()? {
                Ok(r.0[0..self.prefix.len()] == self.prefix)
            } else {
                Ok(false)
            }
        } else if !self.inner.prev()? {
            Ok(false)
        } else if let Some(r) = self.inner.read()? {
            Ok(r.0[0..self.prefix.len()] == self.prefix)
        } else {
            Ok(false)
        }
    }
}
