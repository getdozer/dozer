use std::ops::Bound;

use dozer_types::{
    log::{debug, trace},
    tracing::info,
};
use lmdb::{Database, DatabaseFlags, Transaction, WriteFlags};
use tokio::io::AsyncRead;

use crate::{
    errors::StorageError, generator::FutureGeneratorContext,
    lmdb_database::raw_iterator::RawIterator, yield_return_if_err, DumpItem, RestoreError,
    RwLmdbEnvironment,
};

use super::{
    create_new_database, dump_array, dump_slice, dump_u64, restore_array, restore_slice,
    restore_u64,
};

pub async fn dump<'txn, T: Transaction>(
    txn: &'txn T,
    db: Database,
    flags: DatabaseFlags,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) {
    let count = yield_return_if_err!(context, txn.stat(db)).entries() as u64;
    dump_u64(count, context).await;

    let cursor = yield_return_if_err!(context, txn.open_ro_cursor(db));
    let mut iter = yield_return_if_err!(context, RawIterator::new(cursor, Bound::Unbounded, true));

    let key_len = if flags.contains(DatabaseFlags::INTEGER_KEY) {
        // Dump key length only once
        if let Some(result) = iter.next() {
            let (key, value) = yield_return_if_err!(context, result);
            let key_len = key.len() as u64;
            dump_u64(key_len, context).await;
            dump_array(key, key_len, context).await;
            dump_slice(value, context).await;
            Some(key_len)
        } else {
            None
        }
    } else {
        None
    };

    for result in iter {
        let (key, value) = yield_return_if_err!(context, result);
        if let Some(key_len) = key_len {
            dump_array(key, key_len, context).await;
        } else {
            dump_slice(key, context).await;
        }
        dump_slice(value, context).await;
    }
}

pub async fn restore<'txn, R: AsyncRead + Unpin>(
    env: &mut RwLmdbEnvironment,
    name: &str,
    flags: DatabaseFlags,
    reader: &mut R,
) -> Result<Database, RestoreError> {
    let db = create_new_database(env, name, flags)?;
    info!("Created database {name} with flags {flags:?}");

    let count = restore_u64(reader).await?;
    info!("Restoring {count} items");

    let key_len = if flags.contains(DatabaseFlags::INTEGER_KEY) {
        let key_len = restore_u64(reader).await?;
        info!("Key length is {key_len}");
        Some(key_len)
    } else {
        info!("Key length is variable");
        None
    };

    let txn = env.txn_mut()?;
    info!("Opened transaction");

    for i in 0..count {
        let key = if let Some(key_len) = key_len {
            restore_array(reader, key_len).await?
        } else {
            restore_slice(reader).await?
        };
        debug!("Restored key {i}");
        trace!("Key is {key:?}");

        let value = restore_slice(reader).await?;
        debug!("Restored value {i}");
        trace!("Value is {value:?}");

        txn.put(db, &key, &value, WriteFlags::APPEND)
            .map_err(StorageError::Lmdb)?;
        debug!("Inserted key {i}");
    }

    env.commit()?;
    info!("Committed transaction");

    Ok(db)
}

#[cfg(test)]
mod tests {
    use dozer_types::tonic::async_trait;

    use super::*;

    use super::super::tests::*;

    struct Dumper;

    #[async_trait(?Send)]
    impl Dump for Dumper {
        async fn dump<'txn, T: Transaction>(
            &self,
            txn: &'txn T,
            db: Database,
            flags: DatabaseFlags,
            context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
        ) {
            dump(txn, db, flags, context).await
        }
    }

    struct Restorer;

    #[async_trait(?Send)]
    impl Restore for Restorer {
        async fn restore(
            &self,
            env: &mut RwLmdbEnvironment,
            restore_name: &str,
            flags: DatabaseFlags,
            reader: &mut (impl AsyncRead + Unpin),
        ) -> Result<Database, RestoreError> {
            restore(env, restore_name, flags, reader).await
        }
    }

    #[tokio::test]
    async fn test_dump_no_dup() {
        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"a", b"1"),
            (b"aa", b"2"),
            (b"aaa", b"3"),
            (b"aaaa", b"4"),
            (b"aaaaa", b"5"),
            (b"aaaaaa", b"6"),
            (b"aaaaaaa", b"7"),
            (b"aaaaaaaa", b"8"),
            (b"aaaaaaaaa", b"9"),
            (b"aaaaaaaaaa", b"10"),
        ];
        test_dump_restore(Dumper, Restorer, DatabaseFlags::empty(), &data).await;
    }

    #[tokio::test]
    async fn test_dump_no_dup_integer_key() {
        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"0000", b"1"),
            (b"0001", b"2"),
            (b"0002", b"3"),
            (b"0003", b"4"),
            (b"0004", b"5"),
            (b"0005", b"6"),
            (b"0006", b"7"),
            (b"0007", b"8"),
            (b"0008", b"9"),
            (b"0009", b"10"),
        ];
        test_dump_restore(Dumper, Restorer, DatabaseFlags::empty(), &data).await;
    }
}
