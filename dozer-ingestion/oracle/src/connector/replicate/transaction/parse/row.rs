use std::collections::HashMap;

use regex::Regex;

use crate::connector::Error;

use super::{ParsedRow, ParsedValue};

#[derive(Debug, Clone)]
pub struct Parser {
    regex: Regex,
}

impl Parser {
    pub fn new(delimiter: &str, end: &str) -> Self {
        let regex = Regex::new(&format!(
            "\"(\\w+)\" (= (.+)|IS NULL)({} *\\n|{})",
            delimiter, end
        ))
        .unwrap();
        Self { regex }
    }

    pub fn parse(&self, values: &str) -> Result<ParsedRow, Error> {
        let mut result = HashMap::new();
        for cap in self.regex.captures_iter(values) {
            let column = cap.get(1).unwrap().as_str();
            let value = match cap.get(3) {
                Some(value) => value.as_str().parse()?,
                None => ParsedValue::Null,
            };
            result.insert(column.to_string(), value);
        }
        Ok(result)
    }
}
