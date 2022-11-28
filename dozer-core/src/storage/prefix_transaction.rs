use crate::storage::common::{
    Database, RenewableRwTransaction, RoCursor, RoTransaction, RwCursor, RwTransaction,
};
use crate::storage::errors::StorageError;
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use crate::storage::transactions::SharedTransaction;
use dozer_types::parking_lot::RwLock;
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

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
        Ok(Box::new(PrefixReaderWriterCursor::new(
            cursor,
            self.prefix.clone(),
        )))
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
            Some((key, val)) => Ok(key[0..3] == self.prefix),
            None => Ok(false),
        }
    }

    #[inline]
    fn prev(&self) -> Result<bool, StorageError> {
        if !self.inner.prev()? {
            return Ok(false);
        }
        match self.read()? {
            Some((key, val)) => Ok(key[0..3] == self.prefix),
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
            } else {
                if let Some(r) = self.inner.read()? {
                    Ok(r.0[0..self.prefix.len()] == self.prefix)
                } else {
                    Ok(false)
                }
            }
        } else {
            if !self.inner.prev()? {
                Ok(false)
            } else {
                if let Some(r) = self.inner.read()? {
                    Ok(r.0[0..self.prefix.len()] == self.prefix)
                } else {
                    Ok(false)
                }
            }
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

#[test]
fn test_prefix_tx() {
    let tmp_dir = chk!(TempDir::new("example"));
    if tmp_dir.path().exists() {
        chk!(fs::remove_dir_all(tmp_dir.path()));
    }
    chk!(fs::create_dir(tmp_dir.path()));

    let mut env = chk!(LmdbEnvironmentManager::create(tmp_dir.path(), "test"));
    let db = chk!(env.open_database("test_db", false));
    let tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
        Arc::new(RwLock::new(chk!(env.create_txn())));

    let mut tx0 = tx.clone();
    let mut tx1 = tx.clone();
    let mut tx2 = tx.clone();
    let mut tx3 = tx.clone();

    let mut shared0 = SharedTransaction::new(&tx0);
    let mut shared1 = SharedTransaction::new(&tx1);
    let mut shared2 = SharedTransaction::new(&tx2);
    let mut shared3 = SharedTransaction::new(&tx3);

    let mut ptx0 = PrefixTransaction::new(&mut shared0, 100);
    let mut ptx1 = PrefixTransaction::new(&mut shared1, 101);
    let mut ptx2 = PrefixTransaction::new(&mut shared2, 102);
    let mut ptx3 = PrefixTransaction::new(&mut shared3, 103);

    chk!(ptx0.put(&db, "a0".as_bytes(), "a0".as_bytes()));
    chk!(ptx0.put(&db, "a1".as_bytes(), "a1".as_bytes()));
    chk!(ptx0.put(&db, "a2".as_bytes(), "a2".as_bytes()));

    chk!(ptx1.put(&db, "b0".as_bytes(), "b0".as_bytes()));
    chk!(ptx1.put(&db, "b1".as_bytes(), "b1".as_bytes()));
    chk!(ptx1.put(&db, "b2".as_bytes(), "b2".as_bytes()));

    chk!(ptx2.put(&db, "c0".as_bytes(), "c0".as_bytes()));
    chk!(ptx2.put(&db, "c1".as_bytes(), "c1".as_bytes()));
    chk!(ptx2.put(&db, "c2".as_bytes(), "c2".as_bytes()));

    assert_eq!(
        chk!(ptx0.get(&db, "a0".as_bytes())).unwrap(),
        "a0".as_bytes()
    );
    assert_eq!(chk!(ptx0.get(&db, "b0".as_bytes())), None);

    assert_eq!(
        chk!(ptx1.get(&db, "b0".as_bytes())).unwrap(),
        "b0".as_bytes()
    );
    assert_eq!(chk!(ptx1.get(&db, "a0".as_bytes())), None);

    let ptx1_cur = chk!(ptx1.open_cursor(&db));

    assert!(chk!(ptx1_cur.seek_gte("b1".as_bytes())));
    assert_eq!(chk!(ptx1_cur.read()).unwrap().0, "b1".as_bytes());

    assert!(chk!(ptx1_cur.first()));
    assert_eq!(chk!(ptx1_cur.read()).unwrap().0, "b0".as_bytes());

    assert!(chk!(ptx1_cur.last()));
    assert_eq!(chk!(ptx1_cur.read()).unwrap().0, "b2".as_bytes());

    let ptx2_cur = chk!(ptx2.open_cursor(&db));

    assert!(chk!(ptx2_cur.seek_gte("c1".as_bytes())));
    assert_eq!(chk!(ptx2_cur.read()).unwrap().0, "c1".as_bytes());

    assert!(chk!(ptx2_cur.first()));
    assert_eq!(chk!(ptx2_cur.read()).unwrap().0, "c0".as_bytes());

    assert!(chk!(ptx2_cur.last()));
    assert_eq!(chk!(ptx2_cur.read()).unwrap().0, "c2".as_bytes());

    let ptx0_cur = chk!(ptx0.open_cursor(&db));

    assert!(chk!(ptx0_cur.seek_gte("a1".as_bytes())));
    assert_eq!(chk!(ptx0_cur.read()).unwrap().0, "a1".as_bytes());

    assert!(chk!(ptx0_cur.first()));
    assert_eq!(chk!(ptx0_cur.read()).unwrap().0, "a0".as_bytes());

    assert!(chk!(ptx0_cur.last()));
    assert_eq!(chk!(ptx0_cur.read()).unwrap().0, "a2".as_bytes());

    let ptx3_cur = chk!(ptx3.open_cursor(&db));

    assert!(!chk!(ptx3_cur.seek_gte("a1".as_bytes())));
    assert_eq!(chk!(ptx3_cur.read()), None);
    assert!(!chk!(ptx3_cur.first()));
    assert!(!chk!(ptx3_cur.last()));

    chk!(ptx3_cur.put("d0".as_bytes(), "d1".as_bytes()));
    chk!(ptx3_cur.put("d1".as_bytes(), "d2".as_bytes()));
    chk!(ptx3_cur.put("d2".as_bytes(), "d2".as_bytes()));

    assert!(chk!(ptx3_cur.seek_gte("d1".as_bytes())));
    assert_eq!(chk!(ptx3_cur.read()).unwrap().0, "d1".as_bytes());

    assert!(chk!(ptx3_cur.first()));
    assert_eq!(chk!(ptx3_cur.read()).unwrap().0, "d0".as_bytes());

    assert!(chk!(ptx3_cur.last()));
    assert_eq!(chk!(ptx3_cur.read()).unwrap().0, "d2".as_bytes());
}
