use dozer_types::{
    borrow::{Borrow, Cow},
    types::{IndexDefinition, Record, Schema},
};

use crate::errors::StorageError;

pub enum Encoded<'a> {
    U8([u8; 1]),
    U8x4([u8; 4]),
    U8x8([u8; 8]),
    U8x16([u8; 16]),
    U8x21([u8; 21]),
    Vec(Vec<u8>),
    Borrowed(&'a [u8]),
}

impl<'a> AsRef<[u8]> for Encoded<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::U8(v) => v.as_slice(),
            Self::U8x4(v) => v.as_slice(),
            Self::U8x8(v) => v.as_slice(),
            Self::U8x16(v) => v.as_slice(),
            Self::U8x21(v) => v.as_slice(),
            Self::Vec(v) => v.as_slice(),
            Self::Borrowed(v) => v,
        }
    }
}

pub trait Encode<'a> {
    fn encode(self) -> Result<Encoded<'a>, StorageError>;
}

pub trait BorrowEncode: 'static + for<'a> Borrow<Borrowed<'a> = Self::Encode<'a>> {
    type Encode<'a>: Encode<'a>;
}

pub trait Decode: Borrow {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LmdbValType {
    U32,
    #[cfg(target_pointer_width = "64")]
    U64,
    FixedSizeOtherThanU32OrUsize,
    VariableSize,
}

/// A trait for types that can be used as keys in LMDB.
///
/// # Safety
///
/// - `TYPE` must match the implementation of `encode`.
/// - `decode` must match the implementation of `encode`.
///
/// # Note
///
/// The implementation for `u32` and `u64` has a caveat: The values are encoded in big-endian but compared in native-endian.
pub unsafe trait LmdbKey: BorrowEncode + Decode {
    const TYPE: LmdbValType;
}

/// A trait for types that can be used as values in LMDB.
///
/// # Safety
///
/// - `decode` must match the implementation of `encode`.
pub unsafe trait LmdbValue: BorrowEncode + Decode {}

pub trait LmdbDupValue: LmdbKey + LmdbValue {}

impl<T: LmdbKey + LmdbValue> LmdbDupValue for T {}

impl<'a> Encode<'a> for &'a u8 {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        Ok(Encoded::U8([*self]))
    }
}

impl BorrowEncode for u8 {
    type Encode<'a> = &'a u8;
}

impl Decode for u8 {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Owned(bytes[0]))
    }
}

unsafe impl LmdbKey for u8 {
    const TYPE: LmdbValType = LmdbValType::FixedSizeOtherThanU32OrUsize;
}

unsafe impl LmdbValue for u8 {}

impl<'a> Encode<'a> for &'a u32 {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        Ok(Encoded::U8x4(self.to_be_bytes()))
    }
}

impl BorrowEncode for u32 {
    type Encode<'a> = &'a u32;
}

impl Decode for u32 {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Owned(u32::from_be_bytes(bytes.try_into().unwrap())))
    }
}

unsafe impl LmdbKey for u32 {
    const TYPE: LmdbValType = LmdbValType::U32;
}

unsafe impl LmdbValue for u32 {}

impl<'a> Encode<'a> for &'a u64 {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        Ok(Encoded::U8x8(self.to_be_bytes()))
    }
}

impl BorrowEncode for u64 {
    type Encode<'a> = &'a u64;
}

impl Decode for u64 {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Owned(u64::from_be_bytes(bytes.try_into().unwrap())))
    }
}

unsafe impl LmdbKey for u64 {
    #[cfg(target_pointer_width = "64")]
    const TYPE: LmdbValType = LmdbValType::U64;
    #[cfg(not(target_pointer_width = "64"))]
    const TYPE: LmdbValType = LmdbValType::FixedSizeOtherThanU32OrUsize;
}

unsafe impl LmdbValue for u64 {}

impl<'a> Encode<'a> for &'a [u8] {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        Ok(Encoded::Borrowed(self))
    }
}

impl BorrowEncode for Vec<u8> {
    type Encode<'a> = &'a [u8];
}

impl Decode for Vec<u8> {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Borrowed(bytes))
    }
}

unsafe impl LmdbKey for Vec<u8> {
    const TYPE: LmdbValType = LmdbValType::VariableSize;
}

unsafe impl LmdbValue for Vec<u8> {}

impl<'a> Encode<'a> for &'a str {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        Ok(Encoded::Borrowed(self.as_bytes()))
    }
}

impl BorrowEncode for String {
    type Encode<'a> = &'a str;
}

impl Decode for String {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Borrowed(std::str::from_utf8(bytes).unwrap()))
    }
}

unsafe impl LmdbKey for String {
    const TYPE: LmdbValType = LmdbValType::VariableSize;
}

impl<'a> Encode<'a> for &'a Record {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        dozer_types::bincode::serialize(self)
            .map(Encoded::Vec)
            .map_err(|e| StorageError::SerializationError {
                typ: "Record",
                reason: Box::new(e),
            })
    }
}

impl BorrowEncode for Record {
    type Encode<'a> = &'a Record;
}

impl Decode for Record {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        dozer_types::bincode::deserialize(bytes)
            .map(Cow::Owned)
            .map_err(|e| StorageError::DeserializationError {
                typ: "Record",
                reason: Box::new(e),
            })
    }
}

unsafe impl LmdbKey for Record {
    const TYPE: LmdbValType = LmdbValType::VariableSize;
}

unsafe impl LmdbValue for Record {}

impl<'a> Encode<'a> for &'a (Schema, Vec<IndexDefinition>) {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        dozer_types::bincode::serialize(self)
            .map(Encoded::Vec)
            .map_err(|e| StorageError::SerializationError {
                typ: "(Schema, Vec<IndexDefinition>)",
                reason: Box::new(e),
            })
    }
}

impl BorrowEncode for (Schema, Vec<IndexDefinition>) {
    type Encode<'a> = &'a (Schema, Vec<IndexDefinition>);
}

impl Decode for (Schema, Vec<IndexDefinition>) {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        dozer_types::bincode::deserialize(bytes)
            .map(Cow::Owned)
            .map_err(|e| StorageError::DeserializationError {
                typ: "(Schema, Vec<IndexDefinition>)",
                reason: Box::new(e),
            })
    }
}

unsafe impl LmdbValue for (Schema, Vec<IndexDefinition>) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lmdb_key_types() {
        assert_eq!(u8::TYPE, LmdbValType::FixedSizeOtherThanU32OrUsize);
        assert_eq!(u32::TYPE, LmdbValType::U32);
        assert_eq!(u64::TYPE, LmdbValType::U64);
        assert_eq!(Vec::<u8>::TYPE, LmdbValType::VariableSize);
    }
}
