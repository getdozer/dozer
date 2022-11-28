use crate::storage::common::{Database, RoCursor, RwCursor, RwTransaction};
use crate::storage::errors::StorageError;

pub struct PrefixTransaction<'a> {
    prefix: [u8; 4],
    tx: &'a mut dyn RwTransaction,
}

impl<'a> PrefixTransaction<'a> {
    pub fn new(tx: &'a mut dyn RwTransaction, prefix: u32) -> Self {
        Self {
            tx,
            prefix: prefix.to_be_bytes(),
        }
    }
}

impl<'a> RwTransaction for PrefixTransaction<'a> {
    #[inline]
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.tx.get(db, &full_key)
    }

    #[inline]
    fn put(&mut self, db: &Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.tx.put(db, &full_key, value)
    }

    #[inline]
    fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.tx.del(db, &full_key, value)
    }

    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RwCursor>, StorageError> {
        let cursor = self.tx.open_cursor(db)?;
        Ok(Box::new(PrefixReaderWriterCursor::new(cursor, self.prefix)))
    }
}

pub struct PrefixReaderWriterCursor {
    prefix: [u8; 4],
    inner: Box<dyn RwCursor>,
}

impl PrefixReaderWriterCursor {
    pub fn new(inner: Box<dyn RwCursor>, prefix: [u8; 4]) -> Self {
        Self { inner, prefix }
    }
}

impl RoCursor for PrefixReaderWriterCursor {
    #[inline]
    fn seek_gte(&self, key: &[u8]) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.inner.seek_gte(&full_key)
    }

    #[inline]
    fn seek(&self, key: &[u8]) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.inner.seek(&full_key)
    }

    #[inline]
    fn seek_partial(&self, key: &[u8]) -> Result<bool, StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.inner.seek_partial(&full_key)
    }

    #[inline]
    fn read(&self) -> Result<Option<(&[u8], &[u8])>, StorageError> {
        match self.inner.read()? {
            Some((k, v)) => Ok(Some((&k[self.prefix.len()..], v))),
            None => Ok(None),
        }
    }

    #[inline]
    fn next(&self) -> Result<bool, StorageError> {
        if !self.inner.next()? {
            return Ok(false);
        }
        match self.read()? {
            Some((key, _val)) => Ok(key[0..3] == self.prefix),
            None => Ok(false),
        }
    }

    #[inline]
    fn prev(&self) -> Result<bool, StorageError> {
        if !self.inner.prev()? {
            return Ok(false);
        }
        match self.read()? {
            Some((key, _val)) => Ok(key[0..3] == self.prefix),
            None => Ok(false),
        }
    }

    #[inline]
    fn first(&self) -> Result<bool, StorageError> {
        self.inner.seek_gte(&self.prefix)
    }

    #[inline]
    fn last(&self) -> Result<bool, StorageError> {
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

impl RwCursor for PrefixReaderWriterCursor {
    #[inline]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut full_key = Vec::with_capacity(key.len() + self.prefix.len());
        full_key.extend(self.prefix);
        full_key.extend(key);
        self.inner.put(&full_key, value)
    }
}
