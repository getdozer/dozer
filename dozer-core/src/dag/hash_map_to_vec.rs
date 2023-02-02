use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};

pub fn insert_vec_element<K, V>(map: &mut HashMap<K, Vec<V>>, key: K, value: V)
where
    K: Eq + Hash,
{
    match map.entry(key) {
        Entry::Occupied(mut entry) => {
            entry.get_mut().push(value);
        }
        Entry::Vacant(entry) => {
            entry.insert(vec![value]);
        }
    }
}
