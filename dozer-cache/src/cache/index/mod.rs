use std::cmp::Ordering;

use dozer_types::types::{IndexDefinition, Record};

pub trait CacheIndex {
    // Builds one index based on index definition and record
    fn build(index: &IndexDefinition, rec: &Record) -> Vec<Vec<u8>>;

    fn get_key(schema_id: u32, field_idx: &usize, field_val: &[u8]) -> Vec<u8>;
}

use dozer_types::types::Field;

use crate::errors::CompareError;

pub fn get_primary_key(primary_index: &[usize], values: &[Field]) -> Vec<u8> {
    debug_assert!(
        !primary_index.is_empty(),
        "Primary key indexes cannot be empty"
    );

    let key: Vec<Vec<u8>> = primary_index
        .iter()
        .map(|idx| values[*idx].encode())
        .collect();

    key.join("#".as_bytes())
}

/// Returns the secondary index key for a given set of fields.
///
/// We allow computing the secondary index key of "prefix" fields, so the user can filter the "prefix" fields using `Eq` filters,
/// and sort the filtering result using other fields.
///
/// In the meantime, we compute the key differently for single field indexes and compound indexes.
/// We'are not able to tell if certain `fields` belong to a single field index or compound index if its length is 1, hence the second parameter.
///
/// # Parameters
/// - `fields`: The fields to index.
/// - `is_single_field_index`: Whether the `fields` belong to a single field index. If `true`, `fields` must have length 1.
pub fn get_secondary_index(fields: &[&Field], is_single_field_index: bool) -> Vec<u8> {
    debug_assert!(!is_single_field_index || fields.len() == 1);
    if is_single_field_index {
        fields[0].encode()
    } else {
        get_composite_secondary_index(fields)
    }
}

pub fn compare_composite_secondary_index(a: &[u8], b: &[u8]) -> Result<Ordering, CompareError> {
    let mut a = CompositeSecondaryIndexKey::new(a);
    let mut b = CompositeSecondaryIndexKey::new(b);
    Ok(loop {
        match (a.next(), b.next()) {
            (Some(a), Some(b)) => {
                let a = a?;
                let b = b?;
                match a.cmp(&b) {
                    Ordering::Equal => continue,
                    ordering => break ordering,
                }
            }
            (Some(_), None) => break Ordering::Greater,
            (None, Some(_)) => break Ordering::Less,
            (None, None) => break Ordering::Equal,
        }
    })
}

pub fn get_full_text_secondary_index(token: &str) -> Vec<u8> {
    token.as_bytes().to_vec()
}

fn get_composite_secondary_index(fields: &[&Field]) -> Vec<u8> {
    fn get_field_encoding_len(field: &Field) -> usize {
        8 + field.encoding_len()
    }

    let total_len = fields
        .iter()
        .map(|field| get_field_encoding_len(field))
        .sum::<usize>();
    let mut buf = vec![0; total_len];
    let mut offset = 0;
    for field in fields {
        let field_len = get_field_encoding_len(field);
        buf[offset..offset + 8].copy_from_slice(&(field_len as u64).to_be_bytes());
        field.encode_buf(&mut buf[offset + 8..offset + field_len]);
        offset += field_len;
    }
    buf
}

struct CompositeSecondaryIndexKey<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> CompositeSecondaryIndexKey<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    fn decode_one(&mut self) -> Result<Field, CompareError> {
        if self.offset + 8 > self.buf.len() {
            return Err(CompareError::CannotReadFieldLength);
        }

        let field_len = u64::from_be_bytes(
            self.buf[self.offset..self.offset + 8]
                .try_into()
                .expect("We have checked `buf` is long enough"),
        ) as usize;
        if self.offset + field_len > self.buf.len() {
            return Err(CompareError::CannotReadField);
        }

        let field = Field::decode(&self.buf[self.offset + 8..self.offset + field_len])?;
        self.offset += field_len;
        Ok(field)
    }
}

impl<'a> Iterator for CompositeSecondaryIndexKey<'a> {
    type Item = Result<Field, CompareError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buf.len() {
            return None;
        }

        let result = self.decode_one();
        if result.is_err() {
            // Once an error happens, we stop decoding the rest of the buffer.
            self.offset = self.buf.len();
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests;
