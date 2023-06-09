use std::{ops::ControlFlow, pin::pin};

use dozer_types::log::{debug, info, trace};
use lmdb::{Database, DatabaseFlags, RwTransaction, Transaction, WriteFlags};
use tokio::io::AsyncRead;

use crate::{
    errors::StorageError,
    generator::{FutureGeneratorContext, Generator, IntoGenerator},
    yield_return_if_err, DumpItem, RestoreError, RwLmdbEnvironment,
};

use self::generator::{dup_data, dup_value_counts, DupData};

use super::{
    create_new_database, dump_array, dump_slice, dump_u64, restore_array, restore_slice,
    restore_u64,
};

mod generator;

pub async fn dump<'txn, T: Transaction>(
    txn: &'txn T,
    db: Database,
    flags: DatabaseFlags,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) -> Result<(), ()> {
    let cursor = yield_return_if_err!(context, txn.open_ro_cursor(db));

    // Do one run to find and write the number of keys.
    let mut key_count = 0u64;
    let generator = pin!((|context| dup_data(cursor, context)).into_generator());
    let mut iter = generator.into_iter();
    for result in iter.by_ref() {
        let dup_data = yield_return_if_err!(context, result);
        if matches!(dup_data, DupData::First { .. }) {
            key_count += 1;
        }
    }
    let count_cursor = iter
        .take_return()
        .expect("Generator iterator must have return when exhausted");
    dump_u64(key_count, context).await;
    if key_count == 0 {
        return Ok(());
    }

    // Construct data iterator.
    let data_cursor = yield_return_if_err!(context, txn.open_ro_cursor(db));
    let data_generator = pin!((|context| dup_data(data_cursor, context)).into_generator());
    let mut data_iter = data_generator.into_iter();

    // Find first key value pair.
    let result = data_iter.next().expect("There's at least one key");
    let DupData::First { key, value } = yield_return_if_err!(context, result) else {
        panic!("First item must be `First`");
    };

    // Write first key.
    let key_len = if flags.contains(DatabaseFlags::INTEGER_KEY) {
        let key_len = key.len() as u64;
        dump_u64(key_len, context).await;
        dump_array(key, key_len, context).await;
        Some(key_len)
    } else {
        dump_slice(key, context).await;
        None
    };

    // Construct value count iterator.
    let count_generator =
        pin!((|context| dup_value_counts(count_cursor, context)).into_generator());
    let mut count_iter = count_generator.into_iter();

    // Write first value count.
    let result = count_iter.next().expect("There's at least one key");
    let value_count = yield_return_if_err!(context, result);
    dump_u64(value_count, context).await;

    // Write first value.
    let value_len = if is_value_len_fixed(flags) {
        let value_len = value.len() as u64;
        dump_u64(value_len, context).await;
        dump_array(value, value_len, context).await;
        Some(value_len)
    } else {
        dump_slice(value, context).await;
        None
    };

    // Write following values.
    let (mut key, mut value) = match dump_following_values(&mut data_iter, value_len, context).await
    {
        Ok(ControlFlow::Continue((key, value))) => (key, value),
        Ok(ControlFlow::Break(())) => return Ok(()),
        Err(()) => return Err(()),
    };

    loop {
        // Write key.
        if let Some(key_len) = key_len {
            dump_array(key, key_len, context).await;
        } else {
            dump_slice(key, context).await;
        }

        // Write value count.
        let result = count_iter
            .next()
            .expect("Data iter said there're more keys");
        let value_count = yield_return_if_err!(context, result);
        dump_u64(value_count, context).await;

        // Write first value.
        if let Some(value_len) = value_len {
            dump_array(value, value_len, context).await;
        } else {
            dump_slice(value, context).await;
        }

        // Write following values.
        match dump_following_values(&mut data_iter, value_len, context).await {
            Ok(ControlFlow::Continue((next_key, next_value))) => {
                key = next_key;
                value = next_value;
            }
            Ok(ControlFlow::Break(())) => return Ok(()),
            Err(()) => return Err(()),
        }
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

    // Read key count.
    let key_count = restore_u64(reader).await?;
    info!("Restoring {key_count} keys");
    if key_count == 0 {
        return Ok(db);
    }

    // Read key length.
    let key_len = if flags.contains(DatabaseFlags::INTEGER_KEY) {
        let key_len = restore_u64(reader).await?;
        info!("Key length is {key_len}");
        Some(key_len)
    } else {
        info!("Key length is variable");
        None
    };

    // Read first key.
    let key = if let Some(key_len) = key_len {
        restore_array(reader, key_len).await?
    } else {
        restore_slice(reader).await?
    };
    info!("Restored first key");
    trace!("First key is {:?}", key);

    // Read first value count.
    let value_count = restore_u64(reader).await?;
    info!("First value count is {value_count}");

    // Read value length.
    let value_len = if is_value_len_fixed(flags) {
        let value_len = restore_u64(reader).await?;
        info!("Value length is {value_len}");
        Some(value_len)
    } else {
        info!("Value length is variable");
        None
    };

    let txn = env.txn_mut()?;
    info!("Opened transaction");

    // Read values that belong to the first key.
    restore_values(txn, db, &key, value_count, value_len, reader).await?;

    for i in 1..key_count {
        // Read key.
        let key = if let Some(key_len) = key_len {
            restore_array(reader, key_len).await?
        } else {
            restore_slice(reader).await?
        };
        debug!("Restored key {i}");
        trace!("Key is {:?}", key);

        // Read value count.
        let value_count = restore_u64(reader).await?;
        debug!("Value count is {value_count}");

        // Read values.
        restore_values(txn, db, &key, value_count, value_len, reader).await?;
    }

    env.commit()?;
    info!("Committed transaction");

    Ok(db)
}

/// If the iterator is exhausted, return `Ok(ControlFlow::Break)`.
/// If the iterator is not exhausted, return `Ok(ControlFlow::Continue((key, value)))`, where the key value is the first pair of next key.
/// If there's an error, return `Err(())`.
///
/// Any error is already yielded to the context.
async fn dump_following_values<'txn, I: Iterator<Item = Result<DupData<'txn>, lmdb::Error>>>(
    iter: &mut I,
    value_len: Option<u64>,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) -> Result<ControlFlow<(), (&'txn [u8], &'txn [u8])>, ()> {
    loop {
        match iter.next() {
            Some(Ok(DupData::Following { value })) => {
                if let Some(value_len) = value_len {
                    dump_array(value, value_len, context).await;
                } else {
                    dump_slice(value, context).await;
                }
            }
            Some(Ok(DupData::First { key, value })) => {
                return Ok(ControlFlow::Continue((key, value)));
            }
            Some(Err(e)) => {
                context.yield_(Err(e.into())).await;
                return Err(());
            }
            None => {
                return Ok(ControlFlow::Break(()));
            }
        }
    }
}

async fn restore_values(
    txn: &mut RwTransaction<'_>,
    db: Database,
    key: &[u8],
    value_count: u64,
    value_len: Option<u64>,
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(), RestoreError> {
    for i in 0..value_count {
        let value = if let Some(value_len) = value_len {
            restore_array(reader, value_len).await?
        } else {
            restore_slice(reader).await?
        };
        debug!("Restored value {i}");
        trace!("Value is {value:?}");

        txn.put(db, &key, &value, WriteFlags::empty())
            .map_err(StorageError::Lmdb)?;
        debug!("Inserted value {i}");
    }
    Ok(())
}

fn is_value_len_fixed(flags: DatabaseFlags) -> bool {
    flags.contains(DatabaseFlags::DUP_FIXED) || flags.contains(DatabaseFlags::INTEGER_DUP)
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
            context: FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
        ) -> Result<(), ()> {
            dump(txn, db, flags, &context).await
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
    async fn test_dump_dup() {
        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"a", b"1"),
            (b"a", b"2"),
            (b"aa", b"3"),
            (b"aa", b"4"),
            (b"aaa", b"5"),
            (b"aaa", b"6"),
            (b"aaaa", b"7"),
            (b"aaaa", b"8"),
            (b"aaaaa", b"9"),
            (b"aaaaa", b"10"),
        ];
        test_dump_restore(Dumper, Restorer, DatabaseFlags::DUP_SORT, &data).await;
    }

    #[tokio::test]
    async fn test_dump_dup_integer_key() {
        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"0000", b"1"),
            (b"0000", b"2"),
            (b"0001", b"3"),
            (b"0001", b"4"),
            (b"0002", b"5"),
            (b"0002", b"6"),
            (b"0003", b"7"),
            (b"0003", b"8"),
            (b"0004", b"9"),
            (b"0004", b"10"),
        ];
        test_dump_restore(
            Dumper,
            Restorer,
            DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_KEY,
            &data,
        )
        .await;
    }

    #[tokio::test]
    async fn test_dump_dup_dup_fixed() {
        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"a", b"01"),
            (b"a", b"02"),
            (b"aa", b"03"),
            (b"aa", b"04"),
            (b"aaa", b"05"),
            (b"aaa", b"06"),
            (b"aaaa", b"07"),
            (b"aaaa", b"08"),
            (b"aaaaa", b"09"),
            (b"aaaaa", b"10"),
        ];
        test_dump_restore(
            Dumper,
            Restorer,
            DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            &data,
        )
        .await;
    }

    #[tokio::test]
    async fn test_dump_dup_integer_key_dup_fixed() {
        let data: Vec<(&'static [u8], &'static [u8])> = vec![
            (b"0000", b"01"),
            (b"0000", b"02"),
            (b"0001", b"03"),
            (b"0001", b"04"),
            (b"0002", b"05"),
            (b"0002", b"06"),
            (b"0003", b"07"),
            (b"0003", b"08"),
            (b"0004", b"09"),
            (b"0004", b"10"),
        ];
        test_dump_restore(
            Dumper,
            Restorer,
            DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_FIXED,
            &data,
        )
        .await;
    }

    #[tokio::test]
    async fn test_dump_dup_empty() {
        test_dump_restore(Dumper, Restorer, DatabaseFlags::DUP_SORT, &[]).await;
    }
}
