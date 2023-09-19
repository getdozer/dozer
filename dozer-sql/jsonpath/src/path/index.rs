use crate::parser::model::{FilterExpression, FilterSign, JsonPath};
use crate::path::json::{any_of, eq, inside, less, regex, size, sub_set_of};
use crate::path::top::ObjectField;
use crate::path::{json_path_instance, process_operand, Path, PathInstance};
use crate::JsonPathValue;
use crate::JsonPathValue::{NoValue, Slice};
use dozer_types::json_types::JsonValue;
use dozer_types::json_types::JsonValue::Array;

/// process the slice like [start:end:step]
#[derive(Debug)]
pub(crate) struct ArraySlice {
    start_index: i32,
    end_index: i32,
    step: usize,
}

impl ArraySlice {
    pub(crate) fn new(start_index: i32, end_index: i32, step: usize) -> ArraySlice {
        ArraySlice {
            start_index,
            end_index,
            step,
        }
    }

    fn end(&self, len: i32) -> Option<usize> {
        if self.end_index >= 0 {
            if self.end_index > len {
                None
            } else {
                Some(self.end_index as usize)
            }
        } else if self.end_index < -len {
            None
        } else {
            Some((len - (-self.end_index)) as usize)
        }
    }

    fn start(&self, len: i32) -> Option<usize> {
        if self.start_index >= 0 {
            if self.start_index > len {
                None
            } else {
                Some(self.start_index as usize)
            }
        } else if self.start_index < -len {
            None
        } else {
            Some((len - -self.start_index) as usize)
        }
    }

    fn process<'a, T>(&self, elements: &'a [T]) -> Vec<&'a T> {
        let len = elements.len() as i32;
        let mut filtered_elems: Vec<&T> = vec![];
        match (self.start(len), self.end(len)) {
            (Some(start_idx), Some(end_idx)) => {
                let end_idx = if end_idx == 0 {
                    elements.len()
                } else {
                    end_idx
                };
                for idx in (start_idx..end_idx).step_by(self.step) {
                    if let Some(v) = elements.get(idx) {
                        filtered_elems.push(v)
                    }
                }
                filtered_elems
            }
            _ => filtered_elems,
        }
    }
}

impl<'a> Path<'a> for ArraySlice {
    type Data = JsonValue;

    fn find(&self, input: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        input.flat_map_slice(|data| {
            data.as_array()
                .map(|elems| self.process(elems))
                .and_then(|v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(JsonPathValue::map_vec(v))
                    }
                })
                .unwrap_or_else(|| vec![NoValue])
        })
    }
}

/// process the simple index like [index]
pub(crate) struct ArrayIndex {
    index: usize,
}

impl ArrayIndex {
    pub(crate) fn new(index: usize) -> Self {
        ArrayIndex { index }
    }
}

impl<'a> Path<'a> for ArrayIndex {
    type Data = JsonValue;

    fn find(&self, input: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        input.flat_map_slice(|data| {
            data.as_array()
                .and_then(|elems| elems.get(self.index))
                .map(|e| vec![e.into()])
                .unwrap_or_else(|| vec![NoValue])
        })
    }
}

/// process @ element
pub(crate) struct Current<'a> {
    tail: Option<PathInstance<'a>>,
}

impl<'a> Current<'a> {
    pub(crate) fn from(jp: &'a JsonPath, root: &'a JsonValue) -> Self {
        match jp {
            JsonPath::Empty => Current::none(),
            tail => Current::new(json_path_instance(tail, root)),
        }
    }
    pub(crate) fn new(tail: PathInstance<'a>) -> Self {
        Current { tail: Some(tail) }
    }
    pub(crate) fn none() -> Self {
        Current { tail: None }
    }
}

impl<'a> Path<'a> for Current<'a> {
    type Data = JsonValue;

    fn find(&self, input: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        self.tail
            .as_ref()
            .map(|p| p.find(input.clone()))
            .unwrap_or_else(|| vec![input])
    }
}

/// the list of indexes like [1,2,3]
pub(crate) struct UnionIndex<'a> {
    indexes: Vec<PathInstance<'a>>,
}

impl<'a> UnionIndex<'a> {
    pub fn from_indexes(elems: &'a [JsonValue]) -> Self {
        let mut indexes: Vec<PathInstance<'a>> = vec![];

        for idx in elems.iter() {
            indexes.push(Box::new(ArrayIndex::new(idx.as_u64().unwrap() as usize)))
        }

        UnionIndex::new(indexes)
    }
    pub fn from_keys(elems: &'a [String]) -> Self {
        let mut indexes: Vec<PathInstance<'a>> = vec![];

        for key in elems.iter() {
            indexes.push(Box::new(ObjectField::new(key)))
        }

        UnionIndex::new(indexes)
    }

    pub fn new(indexes: Vec<PathInstance<'a>>) -> Self {
        UnionIndex { indexes }
    }
}

impl<'a> Path<'a> for UnionIndex<'a> {
    type Data = JsonValue;

    fn find(&self, input: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        self.indexes
            .iter()
            .flat_map(|e| e.find(input.clone()))
            .collect()
    }
}

/// process filter element like [?(op sign op)]
pub enum FilterPath<'a> {
    Filter {
        left: PathInstance<'a>,
        right: PathInstance<'a>,
        op: &'a FilterSign,
    },
    Or {
        left: PathInstance<'a>,
        right: PathInstance<'a>,
    },
    And {
        left: PathInstance<'a>,
        right: PathInstance<'a>,
    },
}

impl<'a> FilterPath<'a> {
    pub(crate) fn new(expr: &'a FilterExpression, root: &'a JsonValue) -> Self {
        match expr {
            FilterExpression::Atom(left, op, right) => FilterPath::Filter {
                left: process_operand(left, root),
                right: process_operand(right, root),
                op,
            },
            FilterExpression::And(l, r) => FilterPath::And {
                left: Box::new(FilterPath::new(l, root)),
                right: Box::new(FilterPath::new(r, root)),
            },
            FilterExpression::Or(l, r) => FilterPath::Or {
                left: Box::new(FilterPath::new(l, root)),
                right: Box::new(FilterPath::new(r, root)),
            },
        }
    }
    fn compound(
        one: &'a FilterSign,
        two: &'a FilterSign,
        left: Vec<JsonPathValue<JsonValue>>,
        right: Vec<JsonPathValue<JsonValue>>,
    ) -> bool {
        FilterPath::process_atom(one, left.clone(), right.clone())
            || FilterPath::process_atom(two, left, right)
    }
    fn process_atom(
        op: &'a FilterSign,
        left: Vec<JsonPathValue<JsonValue>>,
        right: Vec<JsonPathValue<JsonValue>>,
    ) -> bool {
        match op {
            FilterSign::Equal => eq(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
            FilterSign::Unequal => !FilterPath::process_atom(&FilterSign::Equal, left, right),
            FilterSign::Less => less(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
            FilterSign::LeOrEq => {
                FilterPath::compound(&FilterSign::Less, &FilterSign::Equal, left, right)
            }
            FilterSign::Greater => !FilterPath::process_atom(&FilterSign::LeOrEq, left, right),
            FilterSign::GrOrEq => !FilterPath::process_atom(&FilterSign::Less, left, right),
            FilterSign::Regex => regex(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
            FilterSign::In => inside(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
            FilterSign::Nin => !FilterPath::process_atom(&FilterSign::In, left, right),
            FilterSign::NoneOf => !FilterPath::process_atom(&FilterSign::AnyOf, left, right),
            FilterSign::AnyOf => any_of(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
            FilterSign::SubSetOf => sub_set_of(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
            FilterSign::Exists => !JsonPathValue::into_data(left).is_empty(),
            FilterSign::Size => size(
                JsonPathValue::into_data(left),
                JsonPathValue::into_data(right),
            ),
        }
    }

    fn process(&self, curr_el: &'a JsonValue) -> bool {
        match self {
            FilterPath::Filter { left, right, op } => {
                FilterPath::process_atom(op, left.find(Slice(curr_el)), right.find(Slice(curr_el)))
            }
            FilterPath::Or { left, right } => {
                if !JsonPathValue::into_data(left.find(Slice(curr_el))).is_empty() {
                    true
                } else {
                    !JsonPathValue::into_data(right.find(Slice(curr_el))).is_empty()
                }
            }
            FilterPath::And { left, right } => {
                if JsonPathValue::into_data(left.find(Slice(curr_el))).is_empty() {
                    false
                } else {
                    !JsonPathValue::into_data(right.find(Slice(curr_el))).is_empty()
                }
            }
        }
    }
}

impl<'a> Path<'a> for FilterPath<'a> {
    type Data = JsonValue;

    fn find(&self, input: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        input.flat_map_slice(|data| {
            let mut res = vec![];
            match data {
                Array(elems) => {
                    for el in elems.iter() {
                        if self.process(el) {
                            res.push(Slice(el))
                        }
                    }
                }
                el => {
                    if self.process(el) {
                        res.push(Slice(el))
                    }
                }
            }
            if res.is_empty() {
                vec![NoValue]
            } else {
                res
            }
        })
    }
}
