use crate::jsonpath::parser::model::{Function, JsonPath, JsonPathIndex, Operand};
use crate::jsonpath::path::index::{ArrayIndex, ArraySlice, Current, FilterPath, UnionIndex};
use crate::jsonpath::path::top::{
    Chain, DescentObject, DescentWildcard, FnPath, IdentityPath, ObjectField, RootPointer, Wildcard,
};
use crate::jsonpath::JsonPathValue;
use dozer_types::json_types::JsonValue;

/// The module is in charge of processing [[JsonPathIndex]] elements
mod index;
/// The module is a helper module providing the set of helping funcitons to process a json elements
mod json;
/// The module is responsible for processing of the [[JsonPath]] elements
mod top;

/// The trait defining the behaviour of processing every separated element.
/// type Data usually stands for json [[JsonValue]]
/// The trait also requires to have a root json to process.
/// It needs in case if in the filter there will be a pointer to the absolute path
pub trait Path<'a> {
    type Data;
    /// when every element needs to handle independently
    fn find(&self, input: JsonPathValue<'a, Self::Data>) -> Vec<JsonPathValue<'a, Self::Data>> {
        vec![input]
    }
    /// when the whole output needs to handle
    fn flat_find(
        &self,
        input: Vec<JsonPathValue<'a, Self::Data>>,
        _is_search_length: bool,
    ) -> Vec<JsonPathValue<'a, Self::Data>> {
        input.into_iter().flat_map(|d| self.find(d)).collect()
    }
    /// defines when we need to invoke `find` or `flat_find`
    fn needs_all(&self) -> bool {
        false
    }
}

/// The basic type for instances.
pub type PathInstance<'a> = Box<dyn Path<'a, Data = JsonValue> + 'a>;

/// The major method to process the top part of json part
pub fn json_path_instance<'a>(json_path: &'a JsonPath, root: &'a JsonValue) -> PathInstance<'a> {
    match json_path {
        JsonPath::Root => Box::new(RootPointer::new(root)),
        JsonPath::Field(key) => Box::new(ObjectField::new(key)),
        JsonPath::Chain(chain) => Box::new(Chain::from(chain, root)),
        JsonPath::Wildcard => Box::new(Wildcard {}),
        JsonPath::Descent(key) => Box::new(DescentObject::new(key)),
        JsonPath::DescentW => Box::new(DescentWildcard),
        JsonPath::Current(value) => Box::new(Current::from(value, root)),
        JsonPath::Index(index) => process_index(index, root),
        JsonPath::Empty => Box::new(IdentityPath {}),
        JsonPath::Fn(Function::Length) => Box::new(FnPath::Size),
    }
}

/// The method processes the indexes(all expressions indie [])
fn process_index<'a>(json_path_index: &'a JsonPathIndex, root: &'a JsonValue) -> PathInstance<'a> {
    match json_path_index {
        JsonPathIndex::Single(index) => Box::new(ArrayIndex::new(index.as_u64().unwrap() as usize)),
        JsonPathIndex::Slice(s, e, step) => Box::new(ArraySlice::new(*s, *e, *step)),
        JsonPathIndex::UnionKeys(elems) => Box::new(UnionIndex::from_keys(elems)),
        JsonPathIndex::UnionIndex(elems) => Box::new(UnionIndex::from_indexes(elems)),
        JsonPathIndex::Filter(fe) => Box::new(FilterPath::new(fe, root)),
    }
}

/// The method processes the operand inside the filter expressions
fn process_operand<'a>(op: &'a Operand, root: &'a JsonValue) -> PathInstance<'a> {
    match op {
        Operand::Static(v) => json_path_instance(&JsonPath::Root, v),
        Operand::Dynamic(jp) => json_path_instance(jp, root),
    }
}
