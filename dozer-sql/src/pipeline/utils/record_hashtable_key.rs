use ahash::AHasher;
use dozer_types::{
    serde::{Deserialize, Serialize},
    types::Field,
};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum RecordKey {
    Accurate(Vec<Field>),
    Hash(u64),
}

pub fn get_record_hash<'a, I>(fields_iter: I) -> u64
where
    I: Iterator<Item = &'a Field>,
{
    let mut hasher = AHasher::default();
    for field in fields_iter {
        field.hash(&mut hasher);
    }
    hasher.finish()
}

#[test]
fn test_record_hash() {
    let record_a = vec![Field::Int(1), Field::String("a".into())];
    let record_b = vec![Field::Int(2), Field::String("b".into())];

    let hash_a = get_record_hash(record_a.iter());
    let hash_b = get_record_hash(record_b.iter());

    assert_ne!(record_a, record_b);
    assert_ne!(hash_a, hash_b);
}
