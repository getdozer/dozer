use std::{borrow::Cow, ops::Bound};

use lmdb::Cursor;

use crate::{errors::StorageError, Decode, Encode, Encoded};

use super::raw_iterator::RawIterator;

pub struct KeyIterator<'txn, 'key, C: Cursor<'txn>, K: ?Sized> {
    inner: RawIterator<'txn, 'key, C>,
    _key: std::marker::PhantomData<*const K>,
}

fn encode_bound<K: Encode + ?Sized>(key: Bound<&K>) -> Result<Bound<Encoded>, StorageError> {
    match key {
        Bound::Included(key) => Ok(Bound::Included(key.encode()?)),
        Bound::Excluded(key) => Ok(Bound::Excluded(key.encode()?)),
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

impl<'txn, 'key, C: Cursor<'txn>, K: Encode + ?Sized> KeyIterator<'txn, 'key, C, K> {
    pub fn new(
        cursor: C,
        starting_key: Bound<&'key K>,
        ascending: bool,
    ) -> Result<Self, StorageError> {
        let starting_key = encode_bound(starting_key)?;
        let inner = RawIterator::new(cursor, starting_key, ascending);
        Ok(Self {
            inner,
            _key: std::marker::PhantomData,
        })
    }
}

fn decode_key<'a, K: Decode + 'a + ?Sized>(key: &'a [u8]) -> Result<Cow<'a, K>, StorageError> {
    let key = K::decode(key).map_err(|e| StorageError::DeserializationError {
        typ: "Iterator::K",
        reason: Box::new(e),
    })?;
    Ok(key)
}

impl<'txn, 'key, C: Cursor<'txn>, K: Decode + 'txn + ?Sized> std::iter::Iterator
    for KeyIterator<'txn, 'key, C, K>
{
    type Item = Result<Cow<'txn, K>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((key, _)) => Some(decode_key(key)),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub struct ValueIterator<'txn, 'key, C: Cursor<'txn>, V: ?Sized> {
    inner: RawIterator<'txn, 'key, C>,
    _value: std::marker::PhantomData<*const V>,
}

impl<'txn, 'key, C: Cursor<'txn>, V: ?Sized> ValueIterator<'txn, 'key, C, V> {
    pub fn new<K: Encode + ?Sized>(
        cursor: C,
        starting_key: Bound<&'key K>,
        ascending: bool,
    ) -> Result<Self, StorageError> {
        let starting_key = encode_bound(starting_key)?;
        let inner = RawIterator::new(cursor, starting_key, ascending);
        Ok(Self {
            inner,
            _value: std::marker::PhantomData,
        })
    }
}

fn decode_value<'a, V: Decode + 'a + ?Sized>(value: &'a [u8]) -> Result<Cow<'a, V>, StorageError> {
    let value = V::decode(value).map_err(|e| StorageError::DeserializationError {
        typ: "Iterator::V",
        reason: Box::new(e),
    })?;
    Ok(value)
}

impl<'txn, 'key, C: Cursor<'txn>, V: Decode + 'txn + ?Sized> std::iter::Iterator
    for ValueIterator<'txn, 'key, C, V>
{
    type Item = Result<Cow<'txn, V>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((_, value)) => Some(decode_value(value)),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub struct Iterator<'txn, 'key, C: Cursor<'txn>, K: ?Sized, V: ?Sized> {
    inner: RawIterator<'txn, 'key, C>,
    _key: std::marker::PhantomData<*const K>,
    _value: std::marker::PhantomData<*const V>,
}

impl<'txn, 'key, C: Cursor<'txn>, K: Encode + ?Sized, V: ?Sized> Iterator<'txn, 'key, C, K, V> {
    pub fn new(
        cursor: C,
        starting_key: Bound<&'key K>,
        ascending: bool,
    ) -> Result<Self, StorageError> {
        let starting_key = encode_bound(starting_key)?;
        let inner = RawIterator::new(cursor, starting_key, ascending);
        Ok(Self {
            inner,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        })
    }
}

fn decode_key_value<'a, K: Decode + 'a + ?Sized, V: Decode + 'a + ?Sized>(
    key: &'a [u8],
    value: &'a [u8],
) -> Result<(Cow<'a, K>, Cow<'a, V>), StorageError> {
    let key = decode_key(key)?;
    let value = decode_value(value)?;
    Ok((key, value))
}

impl<'txn, 'key, C: Cursor<'txn>, K: Decode + 'txn + ?Sized, V: Decode + 'txn + ?Sized>
    std::iter::Iterator for Iterator<'txn, 'key, C, K, V>
{
    type Item = Result<(Cow<'txn, K>, Cow<'txn, V>), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((key, value)) => Some(decode_key_value(key, value)),
            Err(e) => Some(Err(e.into())),
        }
    }
}
