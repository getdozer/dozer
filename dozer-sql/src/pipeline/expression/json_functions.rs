use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{
    InvalidArgument, InvalidFunction, InvalidFunctionArgument, InvalidValue,
};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use jsonpath_rust::{JsonPathFinder, JsonPathInst};

use dozer_types::json_types::{json_value_to_serde_json, serde_json_to_json_value, JsonValue};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record, Schema};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum JsonFunctionType {
    JsonValue,
    JsonQuery,
}

impl Display for JsonFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonFunctionType::JsonValue => f.write_str("JSON_VALUE".to_string().as_str()),
            JsonFunctionType::JsonQuery => f.write_str("JSON_QUERY".to_string().as_str()),
        }
    }
}

impl JsonFunctionType {
    pub(crate) fn new(name: &str) -> Result<JsonFunctionType, PipelineError> {
        match name {
            "json_value" => Ok(JsonFunctionType::JsonValue),
            "json_query" => Ok(JsonFunctionType::JsonQuery),
            _ => Err(InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &Vec<Expression>,
        record: &Record,
    ) -> Result<Field, PipelineError> {
        match self {
            JsonFunctionType::JsonValue => self.evaluate_json_value(schema, args, record),
            JsonFunctionType::JsonQuery => self.evaluate_json_query(schema, args, record),
        }
    }

    pub(crate) fn evaluate_json_value(
        &self,
        schema: &Schema,
        args: &Vec<Expression>,
        record: &Record,
    ) -> Result<Field, PipelineError> {
        if args.len() > 2 {
            return Err(InvalidFunctionArgument(
                self.to_string(),
                args[2].evaluate(record, schema)?,
                2,
            ));
        }
        let json_input = args[0].evaluate(record, schema)?;
        let path = args[1]
            .evaluate(record, schema)?
            .to_string()
            .ok_or(InvalidArgument(args[1].to_string(schema)))?;

        Ok(Field::Json(self.evaluate_json(json_input, path)?))
    }

    pub(crate) fn evaluate_json_query(
        &self,
        schema: &Schema,
        args: &Vec<Expression>,
        record: &Record,
    ) -> Result<Field, PipelineError> {
        let mut path = String::from("$");
        if args.len() < 2 && !args.is_empty() {
            Ok(Field::Json(
                self.evaluate_json(args[0].evaluate(record, schema)?, path)?,
            ))
        } else if args.len() == 2 {
            let json_input = args[0].evaluate(record, schema)?;
            path = args[1]
                .evaluate(record, schema)?
                .to_string()
                .ok_or(InvalidArgument(args[1].to_string(schema)))?;

            Ok(Field::Json(self.evaluate_json(json_input, path)?))
        } else {
            Err(InvalidFunctionArgument(
                self.to_string(),
                args[2].evaluate(record, schema)?,
                2,
            ))
        }
    }

    pub(crate) fn evaluate_json(
        &self,
        json_input: Field,
        path: String,
    ) -> Result<JsonValue, PipelineError> {
        let json_val = json_value_to_serde_json(match json_input.to_json() {
            Some(json) => json.clone(),
            None => JsonValue::Null,
        })
        .map_err(|e| InvalidArgument(e.to_string()))?;

        let finder = JsonPathFinder::new(
            Box::from(json_val),
            Box::from(JsonPathInst::from_str(path.as_str()).map_err(InvalidArgument)?),
        );

        match finder.find() {
            Value::Null => Ok(JsonValue::Null),
            Value::Array(a) => {
                if a.is_empty() {
                    Ok(JsonValue::Array(vec![]))
                } else if a.len() == 1 {
                    let item = match a.first() {
                        Some(i) => i,
                        None => return Err(InvalidValue("Invalid length of array".to_string())),
                    };
                    let value = serde_json_to_json_value(item.clone())
                        .map_err(|e| InvalidValue(e.to_string()))?;
                    Ok(value)
                } else {
                    let mut array_val = vec![];
                    for item in a {
                        array_val.push(
                            serde_json_to_json_value(item)
                                .map_err(|e| InvalidValue(e.to_string()))?,
                        );
                    }
                    Ok(JsonValue::Array(array_val))
                }
            }
            _ => Err(InvalidValue(path)),
        }
    }
}
