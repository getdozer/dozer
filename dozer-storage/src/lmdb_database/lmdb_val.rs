use std::borrow::Cow;

use crate::errors::StorageError;

pub enum Encoded<'a> {
    U8([u8; 1]),
    U8x4([u8; 4]),
    U8x8([u8; 8]),
    Vec(Vec<u8>),
    Borrowed(&'a [u8]),
}

impl<'a> AsRef<[u8]> for Encoded<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::U8(v) => v.as_slice(),
            Self::U8x4(v) => v.as_slice(),
            Self::U8x8(v) => v.as_slice(),
            Self::Vec(v) => v.as_slice(),
            Self::Borrowed(v) => v,
        }
    }
}

pub trait Encode {
    fn encode(&self) -> Result<Encoded, StorageError>;
}

pub trait Decode: ToOwned {
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
/// This trait is `unsafe` because `TYPE` must match the implementation of `encode`.
///
/// # Note
///
/// The implementation for `u32` and `u64` has a caveat: The values are encoded in big-endian but compared in native-endian.
pub unsafe trait LmdbKey: Encode {
    const TYPE: LmdbValType;
}

pub trait LmdbValue: Encode + Decode {}

impl<T: Encode + Decode + ?Sized> LmdbValue for T {}

pub trait LmdbDupValue: LmdbKey + LmdbValue {}

impl<T: LmdbKey + LmdbValue + ?Sized> LmdbDupValue for T {}

impl Encode for u8 {
    fn encode(&self) -> Result<Encoded, StorageError> {
        Ok(Encoded::U8([*self]))
    }
}

impl Decode for u8 {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Owned(bytes[0]))
    }
}

unsafe impl LmdbKey for u8 {
    const TYPE: LmdbValType = LmdbValType::FixedSizeOtherThanU32OrUsize;
}

impl Encode for u32 {
    fn encode(&self) -> Result<Encoded, StorageError> {
        Ok(Encoded::U8x4(self.to_be_bytes()))
    }
}

impl Decode for u32 {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Owned(u32::from_be_bytes(bytes.try_into().unwrap())))
    }
}

unsafe impl LmdbKey for u32 {
    const TYPE: LmdbValType = LmdbValType::U32;
}

impl Encode for u64 {
    fn encode(&self) -> Result<Encoded, StorageError> {
        Ok(Encoded::U8x8(self.to_be_bytes()))
    }
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

impl Encode for [u8] {
    fn encode(&self) -> Result<Encoded, StorageError> {
        Ok(Encoded::Borrowed(self))
    }
}

impl Decode for [u8] {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        Ok(Cow::Borrowed(bytes))
    }
}

unsafe impl LmdbKey for [u8] {
    const TYPE: LmdbValType = LmdbValType::VariableSize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lmdb_key_types() {
        assert_eq!(u8::TYPE, LmdbValType::FixedSizeOtherThanU32OrUsize);
        assert_eq!(u32::TYPE, LmdbValType::U32);
        assert_eq!(u64::TYPE, LmdbValType::U64);
        assert_eq!(<[u8]>::TYPE, LmdbValType::VariableSize);
    }
}
