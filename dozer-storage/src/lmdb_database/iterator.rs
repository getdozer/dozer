use std::ops::Bound;

use dozer_types::borrow::Cow;
use lmdb::Cursor;

use crate::{errors::StorageError, Decode, Encode, Encoded};

use super::{lmdb_val::BorrowEncode, raw_iterator::RawIterator};

pub struct KeyIterator<'txn, C: Cursor<'txn>, K> {
    inner: RawIterator<'txn, C>,
    _key: std::marker::PhantomData<K>,
}

fn encode_bound<'a, K: Encode<'a>>(key: Bound<K>) -> Result<Bound<Encoded<'a>>, StorageError> {
    match key {
        Bound::Included(key) => Ok(Bound::Included(key.encode()?)),
        Bound::Excluded(key) => Ok(Bound::Excluded(key.encode()?)),
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

fn bound_as_ref<'a>(bound: &'a Bound<Encoded<'a>>) -> Bound<&'a [u8]> {
    match bound {
        Bound::Included(key) => Bound::Included(key.as_ref()),
        Bound::Excluded(key) => Bound::Excluded(key.as_ref()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl<'txn, C: Cursor<'txn>, K: BorrowEncode> KeyIterator<'txn, C, K> {
    pub fn new(
        cursor: C,
        starting_key: Bound<K::Encode<'_>>,
        ascending: bool,
    ) -> Result<Self, StorageError> {
        let starting_key = encode_bound(starting_key)?;
        let inner = RawIterator::new(cursor, bound_as_ref(&starting_key), ascending)?;
        Ok(Self {
            inner,
            _key: std::marker::PhantomData,
        })
    }
}

fn decode_key<'a, K: Decode + 'a>(key: &'a [u8]) -> Result<Cow<'a, K>, StorageError> {
    let key = K::decode(key).map_err(|e| StorageError::DeserializationError {
        typ: "Iterator::K",
        reason: Box::new(e),
    })?;
    Ok(key)
}

impl<'txn, C: Cursor<'txn>, K: Decode + 'txn> std::iter::Iterator for KeyIterator<'txn, C, K> {
    type Item = Result<Cow<'txn, K>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((key, _)) => Some(decode_key(key)),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub struct ValueIterator<'txn, C: Cursor<'txn>, V> {
    inner: RawIterator<'txn, C>,
    _value: std::marker::PhantomData<*const V>,
}

impl<'txn, C: Cursor<'txn>, V> ValueIterator<'txn, C, V> {
    pub fn new<K: BorrowEncode>(
        cursor: C,
        starting_key: Bound<K::Encode<'_>>,
        ascending: bool,
    ) -> Result<Self, StorageError> {
        let starting_key = encode_bound(starting_key)?;
        let inner = RawIterator::new(cursor, bound_as_ref(&starting_key), ascending)?;
        Ok(Self {
            inner,
            _value: std::marker::PhantomData,
        })
    }
}

fn decode_value<'a, V: Decode + 'a>(value: &'a [u8]) -> Result<Cow<'a, V>, StorageError> {
    let value = V::decode(value).map_err(|e| StorageError::DeserializationError {
        typ: "Iterator::V",
        reason: Box::new(e),
    })?;
    Ok(value)
}

impl<'txn, C: Cursor<'txn>, V: Decode + 'txn> std::iter::Iterator for ValueIterator<'txn, C, V> {
    type Item = Result<Cow<'txn, V>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((_, value)) => Some(decode_value(value)),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub struct Iterator<'txn, C: Cursor<'txn>, K, V> {
    inner: RawIterator<'txn, C>,
    _key: std::marker::PhantomData<*const K>,
    _value: std::marker::PhantomData<*const V>,
}

impl<'txn, C: Cursor<'txn>, K: BorrowEncode, V> Iterator<'txn, C, K, V> {
    pub fn new<'a>(
        cursor: C,
        starting_key: Bound<K::Encode<'a>>,
        ascending: bool,
    ) -> Result<(Self, Bound<Encoded<'a>>), StorageError> {
        let starting_key = encode_bound(starting_key)?;
        let inner = RawIterator::new(cursor, bound_as_ref(&starting_key), ascending)?;
        Ok((
            Self {
                inner,
                _key: std::marker::PhantomData,
                _value: std::marker::PhantomData,
            },
            starting_key,
        ))
    }
}

fn decode_key_value<'a, K: Decode + 'a, V: Decode + 'a>(
    key: &'a [u8],
    value: &'a [u8],
) -> Result<(Cow<'a, K>, Cow<'a, V>), StorageError> {
    let key = decode_key(key)?;
    let value = decode_value(value)?;
    Ok((key, value))
}

impl<'txn, C: Cursor<'txn>, K: Decode + 'txn, V: Decode + 'txn> std::iter::Iterator
    for Iterator<'txn, C, K, V>
{
    type Item = Result<(Cow<'txn, K>, Cow<'txn, V>), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((key, value)) => Some(decode_key_value(key, value)),
            Err(e) => Some(Err(e.into())),
        }
    }
}
