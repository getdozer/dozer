use std::ops::{Bound, Deref};

use dozer_types::thiserror::{self, Error};
use lmdb::{Database, DatabaseFlags, Transaction};
use tokio::io::{self, AsyncRead, AsyncReadExt};

use crate::{
    errors::StorageError,
    generator::{FutureGeneratorContext, Once},
    yield_return_if_err, LmdbEnvironment, RwLmdbEnvironment,
};

use super::raw_iterator::RawIterator;

#[derive(Debug)]
pub enum DumpItem<'txn> {
    U8x8([u8; 8]),
    U8x4([u8; 4]),
    Slice(&'txn [u8]),
}

impl<'txn> Deref for DumpItem<'txn> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            DumpItem::U8x8(buf) => buf,
            DumpItem::U8x4(buf) => buf,
            DumpItem::Slice(buf) => buf,
        }
    }
}

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("from utf8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid flags: {0}")]
    InvalidFlags(u32),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("database exists: {0}")]
    DatabaseExists(String),
}

pub async fn dump<'txn, T: Transaction>(
    txn: &'txn T,
    name: &'txn str,
    db: Database,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) -> Result<(), ()> {
    dump_string(name, context).await;

    let flags = yield_return_if_err!(context, txn.db_flags(db));
    dump_u32(flags.bits(), context).await;

    if flags.contains(DatabaseFlags::DUP_SORT) {
        dup::dump(txn, db, flags, context).await
    } else {
        no_dup::dump(txn, db, flags, context).await
    }
}

pub async fn restore<'txn, R: AsyncRead + Unpin>(
    env: &mut RwLmdbEnvironment,
    reader: &mut R,
) -> Result<Database, RestoreError> {
    let name = restore_string(reader).await?;

    let flags = restore_u32(reader).await?;
    let flags = DatabaseFlags::from_bits(flags).ok_or(RestoreError::InvalidFlags(flags))?;

    if flags.contains(DatabaseFlags::DUP_SORT) {
        dup::restore(env, &name, flags, reader).await
    } else {
        no_dup::restore(env, &name, flags, reader).await
    }
}

fn create_new_database(
    env: &mut RwLmdbEnvironment,
    name: &str,
    flags: DatabaseFlags,
) -> Result<Database, RestoreError> {
    match env.open_database(Some(name)) {
        Ok(_) => Err(RestoreError::DatabaseExists(name.to_owned())),
        Err(StorageError::Lmdb(lmdb::Error::NotFound)) => {
            env.create_database(Some(name), flags).map_err(Into::into)
        }
        Err(e) => Err(e.into()),
    }
}

mod dup;
mod no_dup;

fn dump_u64(
    value: u64,
    context: &FutureGeneratorContext<Result<DumpItem<'_>, StorageError>>,
) -> Once {
    context.yield_(Ok(DumpItem::U8x8(value.to_le_bytes())))
}

async fn restore_u64(reader: &mut (impl AsyncRead + Unpin)) -> Result<u64, RestoreError> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

fn dump_u32(
    value: u32,
    context: &FutureGeneratorContext<Result<DumpItem<'_>, StorageError>>,
) -> Once {
    context.yield_(Ok(DumpItem::U8x4(value.to_le_bytes())))
}

async fn restore_u32(reader: &mut (impl AsyncRead + Unpin)) -> Result<u32, RestoreError> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    Ok(u32::from_le_bytes(buf))
}

fn dump_array<'a>(
    array: &'a [u8],
    len: u64,
    context: &FutureGeneratorContext<Result<DumpItem<'a>, StorageError>>,
) -> Once {
    debug_assert!(array.len() as u64 == len);
    context.yield_(Ok(DumpItem::Slice(array)))
}

async fn restore_array(
    reader: &mut (impl AsyncRead + Unpin),
    len: u64,
) -> Result<Vec<u8>, RestoreError> {
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn dump_string<'a>(
    string: &'a str,
    context: &FutureGeneratorContext<Result<DumpItem<'a>, StorageError>>,
) {
    dump_slice(string.as_bytes(), context).await;
}

async fn restore_string(reader: &mut (impl AsyncRead + Unpin)) -> Result<String, RestoreError> {
    let buf = restore_slice(reader).await?;
    String::from_utf8(buf).map_err(Into::into)
}

async fn dump_slice<'a>(
    slice: &'a [u8],
    context: &FutureGeneratorContext<Result<DumpItem<'a>, StorageError>>,
) {
    dump_u64(slice.len() as u64, context).await;
    context.yield_(Ok(DumpItem::Slice(slice))).await;
}

async fn restore_slice(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>, RestoreError> {
    let len = restore_u64(reader).await?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
pub mod tests {
    use std::pin::pin;

    use super::*;

    use async_trait::async_trait;
    use lmdb::WriteFlags;
    use tempdir::TempDir;

    use crate::{
        generator::{Generator, IntoGenerator},
        lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
    };

    fn create_env() -> (TempDir, RwLmdbEnvironment) {
        let temp_dir = TempDir::new("test").unwrap();
        let env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "dump_tests_env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        (temp_dir, env)
    }

    fn insert_data(env: &mut RwLmdbEnvironment, db: Database, data: &[(&[u8], &[u8])]) {
        let txn = env.txn_mut().unwrap();
        for (key, value) in data {
            txn.put(db, key, value, WriteFlags::empty()).unwrap();
        }
        env.commit().unwrap();
    }

    #[async_trait(?Send)]
    pub trait Dump {
        async fn dump<'txn, T: Transaction>(
            &self,
            txn: &'txn T,
            db: Database,
            flags: DatabaseFlags,
            context: FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
        ) -> Result<(), ()>;
    }

    async fn dump_database(
        dumper: impl Dump,
        env: &mut RwLmdbEnvironment,
        db: Database,
        flags: DatabaseFlags,
    ) -> Vec<u8> {
        let mut result = vec![];
        let txn = &env.begin_txn().unwrap();
        let generator = (|context| dumper.dump(txn, db, flags, context)).into_generator();
        for item in pin!(generator).into_iter() {
            result.extend_from_slice(&item.unwrap());
        }
        result
    }

    #[async_trait(?Send)]
    pub trait Restore {
        async fn restore(
            &self,
            env: &mut RwLmdbEnvironment,
            restore_name: &str,
            flags: DatabaseFlags,
            reader: &mut (impl AsyncRead + Unpin),
        ) -> Result<Database, RestoreError>;
    }

    async fn restore_database(
        restorer: impl Restore,
        env: &mut RwLmdbEnvironment,
        restore_name: &str,
        flags: DatabaseFlags,
        mut data: &[u8],
    ) -> Database {
        restorer
            .restore(env, restore_name, flags, &mut data)
            .await
            .unwrap()
    }

    async fn dump_restore_database(
        dumper: impl Dump,
        restorer: impl Restore,
        env: &mut RwLmdbEnvironment,
        db: Database,
        flags: DatabaseFlags,
        restore_name: &str,
    ) -> Database {
        let data = dump_database(dumper, env, db, flags).await;
        restore_database(restorer, env, restore_name, flags, &data).await
    }

    pub async fn test_dump_restore(
        dumper: impl Dump,
        restorer: impl Restore,
        flags: DatabaseFlags,
        data: &[(&[u8], &[u8])],
    ) {
        // Create database.
        let (_temp_dir, mut env) = create_env();
        let db = env
            .create_database(Some("test_dump_restore"), flags)
            .unwrap();
        insert_data(&mut env, db, data);

        // Do dump-restore round trip.
        let restore_db = dump_restore_database(
            dumper,
            restorer,
            &mut env,
            db,
            flags,
            "test_dump_restore_restore",
        )
        .await;

        // Check the restored database.
        let txn = env.begin_txn().unwrap();
        assert_database_equal(&txn, db, &txn, restore_db);
    }
}

pub fn assert_database_equal<T1: Transaction, T2: Transaction>(
    txn1: &T1,
    db1: Database,
    txn2: &T2,
    db2: Database,
) {
    assert_eq!(
        txn1.stat(db1).unwrap().entries(),
        txn2.stat(db2).unwrap().entries()
    );
    let cursor1 = txn1.open_ro_cursor(db1).unwrap();
    let cursor2 = txn2.open_ro_cursor(db2).unwrap();
    let iter1 = RawIterator::new(cursor1, Bound::Unbounded, true).unwrap();
    let iter2 = RawIterator::new(cursor2, Bound::Unbounded, true).unwrap();
    for (result1, result2) in iter1.zip(iter2) {
        let (key1, value1) = result1.unwrap();
        let (key2, value2) = result2.unwrap();
        assert_eq!(key1, key2);
        assert_eq!(value1, value2);
    }
}
