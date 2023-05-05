#![allow(clippy::vec_init_then_push)]

use crate::jsonpath::parser::model::JsonPath;
use crate::jsonpath::path::{json_path_instance, PathInstance};
use crate::jsonpath::JsonPathValue::{NewValue, NoValue, Slice};
use dozer_types::json_types::JsonValue;
use std::convert::TryInto;
use std::fmt::Debug;
use std::str::FromStr;

pub mod parser;
pub mod path;

pub trait JsonPathQuery {
    fn path(self, query: &str) -> Result<JsonValue, String>;
}

pub struct JsonPathInst {
    inner: JsonPath,
}

impl FromStr for JsonPathInst {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(JsonPathInst {
            inner: s.try_into()?,
        })
    }
}

impl JsonPathQuery for Box<JsonValue> {
    fn path(self, query: &str) -> Result<JsonValue, String> {
        let p = JsonPathInst::from_str(query)?;
        Ok(JsonPathFinder::new(self, Box::new(p)).find())
    }
}

impl JsonPathQuery for JsonValue {
    fn path(self, query: &str) -> Result<JsonValue, String> {
        let p = JsonPathInst::from_str(query)?;
        Ok(JsonPathFinder::new(Box::new(self), Box::new(p)).find())
    }
}

#[macro_export]
macro_rules! json_path_value {
    (&$v:expr) =>{
        JsonPathValue::Slice(&$v)
    };

    ($(&$v:expr),+ $(,)?) =>{
        {
        let mut res = Vec::new();
        $(
           res.push(json_path_value!(&$v));
        )+
        res
        }
    };
    ($v:expr) =>{
        JsonPathValue::NewValue($v)
    };

}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum JsonPathValue<'a, Data> {
    /// The slice of the initial json data
    Slice(&'a Data),
    /// The new data that was generated from the input data (like length operator)
    NewValue(Data),
    /// The absent value that indicates the input data is not matched to the given json path (like the absent fields)
    NoValue,
}

impl<'a, Data: Clone + Debug + Default> JsonPathValue<'a, Data> {
    pub fn to_data(self) -> Data {
        match self {
            Slice(r) => r.clone(),
            NewValue(val) => val,
            NoValue => Data::default(),
        }
    }
}

impl<'a, Data> From<&'a Data> for JsonPathValue<'a, Data> {
    fn from(data: &'a Data) -> Self {
        Slice(data)
    }
}

impl<'a, Data> JsonPathValue<'a, Data> {
    fn only_no_value(input: &Vec<JsonPathValue<'a, Data>>) -> bool {
        !input.is_empty() && input.iter().filter(|v| v.has_value()).count() == 0
    }

    fn map_vec(data: Vec<&'a Data>) -> Vec<JsonPathValue<'a, Data>> {
        data.into_iter().map(|v| v.into()).collect()
    }

    fn map_slice<F>(self, mapper: F) -> Vec<JsonPathValue<'a, Data>>
    where
        F: FnOnce(&'a Data) -> Vec<&'a Data>,
    {
        match self {
            Slice(r) => mapper(r).into_iter().map(Slice).collect(),
            NewValue(_) => vec![],
            no_v => vec![no_v],
        }
    }

    fn flat_map_slice<F>(self, mapper: F) -> Vec<JsonPathValue<'a, Data>>
    where
        F: FnOnce(&'a Data) -> Vec<JsonPathValue<'a, Data>>,
    {
        match self {
            Slice(r) => mapper(r),
            _ => vec![NoValue],
        }
    }

    pub fn has_value(&self) -> bool {
        !matches!(self, NoValue)
    }

    pub fn into_data(input: Vec<JsonPathValue<'a, Data>>) -> Vec<&'a Data> {
        input
            .into_iter()
            .filter_map(|v| match v {
                Slice(el) => Some(el),
                _ => None,
            })
            .collect()
    }

    /// moves a pointer (from slice) out or provides a default value when the value was generated
    pub fn slice_or(self, default: &'a Data) -> &'a Data {
        match self {
            Slice(r) => r,
            NewValue(_) | NoValue => default,
        }
    }
}

/// The base structure stitching the json instance and jsonpath instance
pub struct JsonPathFinder {
    json: Box<JsonValue>,
    path: Box<JsonPathInst>,
}

impl JsonPathFinder {
    /// creates a new instance of [JsonPathFinder]
    pub fn new(json: Box<JsonValue>, path: Box<JsonPathInst>) -> Self {
        JsonPathFinder { json, path }
    }

    /// updates a path with a new one
    pub fn set_path(&mut self, path: Box<JsonPathInst>) {
        self.path = path
    }
    /// updates a json with a new one
    pub fn set_json(&mut self, json: Box<JsonValue>) {
        self.json = json
    }
    /// updates a json from string and therefore can be some parsing errors
    pub fn set_json_str(&mut self, json: &str) -> Result<(), String> {
        self.json = Box::from(JsonValue::from_str(json).map_err(|e| e.to_string())?);
        Ok(())
    }
    /// updates a path from string and therefore can be some parsing errors
    pub fn set_path_str(&mut self, path: &str) -> Result<(), String> {
        self.path = Box::new(JsonPathInst::from_str(path)?);
        Ok(())
    }

    /// create a new instance from string and therefore can be some parsing errors
    pub fn from_str(json: &str, path: &str) -> Result<Self, String> {
        let json = JsonValue::from_str(json).map_err(|e| e.to_string())?;
        let path = Box::new(JsonPathInst::from_str(path)?);
        Ok(JsonPathFinder::new(Box::from(json), path))
    }

    /// creates an instance to find a json slice from the json
    pub fn instance(&self) -> PathInstance {
        json_path_instance(&self.path.inner, &self.json)
    }
    /// finds a slice of data in the set json.
    /// The result is a vector of references to the incoming structure.
    pub fn find_slice(&self) -> Vec<JsonPathValue<'_, JsonValue>> {
        let res = self.instance().find((&(*self.json)).into());
        let has_v: Vec<JsonPathValue<'_, JsonValue>> =
            res.into_iter().filter(|v| v.has_value()).collect();

        if has_v.is_empty() {
            vec![NoValue]
        } else {
            has_v
        }
    }

    /// finds a slice of data and wrap it with Value::Array by cloning the data.
    /// Returns either an array of elements or Json::Null if the match is incorrect.
    pub fn find(&self) -> JsonValue {
        let slice = self.find_slice();
        if !slice.is_empty() {
            if JsonPathValue::only_no_value(&slice) {
                JsonValue::Null
            } else {
                JsonValue::Array(
                    self.find_slice()
                        .into_iter()
                        .filter(|v| v.has_value())
                        .map(|v| v.to_data())
                        .collect(),
                )
            }
        } else {
            JsonValue::Array(vec![])
        }
    }
}
