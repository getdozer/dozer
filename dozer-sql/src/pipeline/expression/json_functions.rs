use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{
    InvalidArgument, InvalidFunction, InvalidFunctionArgument, InvalidValue,
};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use jsonpath_rust::JsonPathQuery;

use dozer_types::json_types::{json_value_to_serde_json, serde_json_to_json_value, JsonValue};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record, Schema};
use std::fmt::{Display, Formatter};

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

        Ok(Field::Json(
            serde_json_to_json_value(self.evaluate_json(json_input, path)?)
                .map_err(|e| InvalidValue(e.to_string()))?,
        ))
    }

    pub(crate) fn evaluate_json_query(
        &self,
        schema: &Schema,
        args: &Vec<Expression>,
        record: &Record,
    ) -> Result<Field, PipelineError> {
        let mut path = String::from("$");
        if args.len() < 2 && !args.is_empty() {
            return Ok(Field::Json(
                serde_json_to_json_value(
                    self.evaluate_json(args[0].evaluate(record, schema)?, path)?,
                )
                .map_err(|e| InvalidValue(e.to_string()))?,
            ));
        } else if args.len() == 2 {
            let json_input = args[0].evaluate(record, schema)?;
            path = args[1]
                .evaluate(record, schema)?
                .to_string()
                .ok_or(InvalidArgument(args[1].to_string(schema)))?;

            return Ok(Field::Json(
                serde_json_to_json_value(self.evaluate_json(json_input, path)?)
                    .map_err(|e| InvalidValue(e.to_string()))?,
            ));
        }

        Err(InvalidFunctionArgument(
            self.to_string(),
            args[2].evaluate(record, schema)?,
            2,
        ))
    }

    pub(crate) fn evaluate_json(
        &self,
        json_input: Field,
        path: String,
    ) -> Result<Value, PipelineError> {
        let json_val = json_value_to_serde_json(match json_input.to_json() {
            Some(json) => json.clone(),
            None => JsonValue::Null,
        })
        .map_err(|e| InvalidArgument(e.to_string()))?;

        let res = json_val.path(path.as_str()).map_err(InvalidArgument)?;

        return match json_input.to_json().unwrap() {
            JsonValue::Array(_) => Ok(res),
            _ => match res {
                Value::Array(array) => {
                    return if array.len() == 1 {
                        match array.get(0) {
                            Some(r) => Ok(r.clone()),
                            None => Err(InvalidFunction(self.to_string())),
                        }
                    } else {
                        Err(InvalidFunction(self.to_string()))
                    }
                }
                _ => Err(InvalidValue(path)),
            },
        };
    }
}
