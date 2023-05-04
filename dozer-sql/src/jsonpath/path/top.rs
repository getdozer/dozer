use crate::jsonpath::parser::model::*;
use crate::jsonpath::path::JsonPathValue::{NewValue, NoValue, Slice};
use crate::jsonpath::path::{json_path_instance, JsonPathValue, Path, PathInstance};
use dozer_types::json_types::JsonValue::{Array, Object};
use dozer_types::json_types::{serde_json_to_json_value, JsonValue};
use dozer_types::serde_json::json;

/// to process the element [*]
pub(crate) struct Wildcard {}

impl<'a> Path<'a> for Wildcard {
    type Data = JsonValue;

    fn find(&self, data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        data.flat_map_slice(|data| {
            let res = match data {
                Array(elems) => {
                    let mut res = vec![];
                    for el in elems.iter() {
                        res.push(Slice(el));
                    }

                    res
                }
                Object(elems) => {
                    let mut res = vec![];
                    for el in elems.values() {
                        res.push(Slice(el));
                    }
                    res
                }
                _ => vec![],
            };
            if res.is_empty() {
                vec![NoValue]
            } else {
                res
            }
        })
    }
}

/// empty path. Returns incoming data.
pub(crate) struct IdentityPath {}

impl<'a> Path<'a> for IdentityPath {
    type Data = JsonValue;

    fn find(&self, data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        vec![data]
    }
}

pub(crate) struct EmptyPath {}

impl<'a> Path<'a> for EmptyPath {
    type Data = JsonValue;

    fn find(&self, _data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        vec![]
    }
}

/// process $ element
pub(crate) struct RootPointer<'a, T> {
    root: &'a T,
}

impl<'a, T> RootPointer<'a, T> {
    pub(crate) fn new(root: &'a T) -> RootPointer<'a, T> {
        RootPointer { root }
    }
}

impl<'a> Path<'a> for RootPointer<'a, JsonValue> {
    type Data = JsonValue;

    fn find(&self, _data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        vec![Slice(self.root)]
    }
}

/// process object fields like ['key'] or .key
pub(crate) struct ObjectField<'a> {
    key: &'a str,
}

impl<'a> ObjectField<'a> {
    pub(crate) fn new(key: &'a str) -> ObjectField<'a> {
        ObjectField { key }
    }
}

impl<'a> Clone for ObjectField<'a> {
    fn clone(&self) -> Self {
        ObjectField::new(self.key)
    }
}

impl<'a> Path<'a> for FnPath {
    type Data = JsonValue;

    fn flat_find(
        &self,
        input: Vec<JsonPathValue<'a, Self::Data>>,
        is_search_length: bool,
    ) -> Vec<JsonPathValue<'a, Self::Data>> {
        // todo rewrite
        if JsonPathValue::only_no_value(&input) {
            return vec![NoValue];
        }

        let res = if is_search_length {
            NewValue(
                serde_json_to_json_value(json!(input.iter().filter(|v| v.has_value()).count()))
                    .unwrap(),
            )
        } else {
            let take_len = |v: &JsonValue| match v {
                Array(elems) => NewValue(serde_json_to_json_value(json!(elems.len())).unwrap()),
                _ => NoValue,
            };

            match input.get(0) {
                Some(v) => match v {
                    NewValue(d) => take_len(d),
                    Slice(s) => take_len(s),
                    NoValue => NoValue,
                },
                None => NoValue,
            }
        };
        vec![res]
    }

    fn needs_all(&self) -> bool {
        true
    }
}

pub(crate) enum FnPath {
    Size,
}

impl<'a> Path<'a> for ObjectField<'a> {
    type Data = JsonValue;

    fn find(&self, data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        let take_field = |v: &'a JsonValue| match v {
            Object(fields) => fields.get(self.key),
            _ => None,
        };

        let res = match data {
            Slice(js) => take_field(js).map(Slice).unwrap_or_else(|| NoValue),
            _ => NoValue,
        };
        vec![res]
    }
}
/// the top method of the processing ..*
pub(crate) struct DescentWildcard;

impl<'a> Path<'a> for DescentWildcard {
    type Data = JsonValue;

    fn find(&self, data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        data.map_slice(deep_flatten)
    }
}

// todo rewrite to tail rec
fn deep_flatten(data: &JsonValue) -> Vec<&JsonValue> {
    let mut acc = vec![];
    match data {
        Object(elems) => {
            for v in elems.values() {
                acc.push(v);
                acc.append(&mut deep_flatten(v));
            }
        }
        Array(elems) => {
            for v in elems.iter() {
                acc.push(v);
                acc.append(&mut deep_flatten(v));
            }
        }
        _ => (),
    }
    acc
}

// todo rewrite to tail rec
fn deep_path_by_key<'a>(data: &'a JsonValue, key: ObjectField<'a>) -> Vec<&'a JsonValue> {
    let mut level: Vec<&JsonValue> = JsonPathValue::into_data(key.find(data.into()));
    match data {
        Object(elems) => {
            let mut next_levels: Vec<&JsonValue> = elems
                .values()
                .flat_map(|v| deep_path_by_key(v, key.clone()))
                .collect();
            level.append(&mut next_levels);
            level
        }
        Array(elems) => {
            let mut next_levels: Vec<&JsonValue> = elems
                .iter()
                .flat_map(|v| deep_path_by_key(v, key.clone()))
                .collect();
            level.append(&mut next_levels);
            level
        }
        _ => level,
    }
}

/// processes decent object like ..
pub(crate) struct DescentObject<'a> {
    key: &'a str,
}

impl<'a> Path<'a> for DescentObject<'a> {
    type Data = JsonValue;

    fn find(&self, data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        data.flat_map_slice(|data| {
            let res_col = deep_path_by_key(data, ObjectField::new(self.key));
            if res_col.is_empty() {
                vec![NoValue]
            } else {
                JsonPathValue::map_vec(res_col)
            }
        })
    }
}

impl<'a> DescentObject<'a> {
    pub fn new(key: &'a str) -> Self {
        DescentObject { key }
    }
}

/// the top method of the processing representing the chain of other operators
pub(crate) struct Chain<'a> {
    chain: Vec<PathInstance<'a>>,
    is_search_length: bool,
}

impl<'a> Chain<'a> {
    pub fn new(chain: Vec<PathInstance<'a>>, is_search_length: bool) -> Self {
        Chain {
            chain,
            is_search_length,
        }
    }
    pub fn from(chain: &'a [JsonPath], root: &'a JsonValue) -> Self {
        let chain_len = chain.len();
        let is_search_length = if chain_len > 2 {
            let mut res = false;
            // if the result of the slice expected to be a slice, union or filter -
            // length should return length of resulted array
            // In all other cases, including single index, we should fetch item from resulting array
            // and return length of that item
            res = match chain.get(chain_len - 1).expect("chain element disappeared") {
                JsonPath::Fn(Function::Length) => {
                    for item in chain.iter() {
                        match (item, res) {
                            // if we found union, slice, filter or wildcard - set search to true
                            (
                                JsonPath::Index(JsonPathIndex::UnionIndex(_))
                                | JsonPath::Index(JsonPathIndex::UnionKeys(_))
                                | JsonPath::Index(JsonPathIndex::Slice(_, _, _))
                                | JsonPath::Index(JsonPathIndex::Filter(_))
                                | JsonPath::Wildcard,
                                false,
                            ) => {
                                res = true;
                            }
                            // if we found a fetching of single index - reset search to false
                            (JsonPath::Index(JsonPathIndex::Single(_)), true) => {
                                res = false;
                            }
                            (_, _) => {}
                        }
                    }
                    res
                }
                _ => false,
            };
            res
        } else {
            false
        };

        Chain::new(
            chain.iter().map(|p| json_path_instance(p, root)).collect(),
            is_search_length,
        )
    }
}

impl<'a> Path<'a> for Chain<'a> {
    type Data = JsonValue;

    fn find(&self, data: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        let mut res = vec![data];

        for inst in self.chain.iter() {
            if inst.needs_all() {
                res = inst.flat_find(res, self.is_search_length)
            } else {
                res = res.into_iter().flat_map(|d| inst.find(d)).collect()
            }
        }
        res
    }
}
