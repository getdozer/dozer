use crate::arg_utils::validate_num_arguments;
use crate::error::Error;
use crate::execution::Expression;

use dozer_types::json_types::{field_to_json_value, json_value_to_serde_json, JsonValue, serde_json_to_json_value};
use dozer_types::types::{FieldType, Record};
use dozer_types::types::{Field, Schema};
use jsonpath::{JsonPathFinder, JsonPathInst};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum JsonFunctionType {
    JsonValue,
    JsonQuery,
    JsonObject,
}

impl Display for JsonFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonFunctionType::JsonValue => f.write_str("JSON_VALUE".to_string().as_str()),
            JsonFunctionType::JsonQuery => f.write_str("JSON_QUERY".to_string().as_str()),
            JsonFunctionType::JsonObject => f.write_str("JSON_OBJECT".to_string().as_str()),
        }
    }
}

impl JsonFunctionType {
    pub(crate) fn new(name: &str) -> Option<JsonFunctionType> {
        match name {
            "json_value" => Some(JsonFunctionType::JsonValue),
            "json_query" => Some(JsonFunctionType::JsonQuery),
            "json_object" => Some(JsonFunctionType::JsonObject),
            _ => None,
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &mut [Expression],
        record: &Record,
    ) -> Result<Field, Error> {
        match self {
            JsonFunctionType::JsonValue => self.evaluate_json_value(schema, args, record),
            JsonFunctionType::JsonQuery => self.evaluate_json_query(schema, args, record),
            JsonFunctionType::JsonObject => self.evaluate_json_object(schema, args, record),
        }
    }

    pub(crate) fn evaluate_json_object(
        &self,
        schema: &Schema,
        args: &mut [Expression],
        record: &Record,
    ) -> Result<Field, Error> {
        let mut json_output = serde_json::Map::new();
        for arg in args {
            if arg.get_type(schema)?.return_type == FieldType::String {
                let arg_string = arg.to_string(schema);
                let object_name: &str = arg_string.split(":").collect::<Vec<&str>>()[0].trim();
                let mut object_value: &str = arg_string.split(":").collect::<Vec<&str>>()[1].trim();
                if object_value.contains(".") {
                    object_value = object_value.split(".").collect::<Vec<&str>>()[1];
                }
                let column_num = schema.fields.iter().position(|x| x.name == object_value);
                if let Some(idx) = column_num {
                    let val = Expression::Column {index: idx}.evaluate(record, schema)?;
                    json_output.insert(
                        object_name.to_string(),
                        json_value_to_serde_json(&field_to_json_value(val))
                    );
                } else {
                    json_output.insert(
                        object_name.to_string(),
                        json_value_to_serde_json(&field_to_json_value(Field::String(object_value.to_string())))
                    );
                }
            }
        }
        Ok(Field::Json(serde_json_to_json_value(Value::Object(json_output))?))
    }

    pub(crate) fn evaluate_json_value(
        &self,
        schema: &Schema,
        args: &mut [Expression],
        record: &Record,
    ) -> Result<Field, Error> {
        validate_num_arguments(2..3, args.len(), self)?;
        let json_input = args[0].evaluate(record, schema)?;
        let path = args[1].evaluate(record, schema)?.to_string();

        if let Ok(json_value) = self.evaluate_json(json_input, path) {
            if json_value.is_string() || json_value.is_number() || json_value.is_bool() {
                return Ok(Field::Json(json_value));
            }
            Ok(Field::Json(JsonValue::NULL))
        } else {
            Ok(Field::Null)
        }
    }

    pub(crate) fn evaluate_json_query(
        &self,
        schema: &Schema,
        args: &mut [Expression],
        record: &Record,
    ) -> Result<Field, Error> {
        validate_num_arguments(1..3, args.len(), self)?;
        if args.len() == 1 {
            Ok(Field::Json(self.evaluate_json(
                args[0].evaluate(record, schema)?,
                String::from("$"),
            )?))
        } else {
            let json_input = args[0].evaluate(record, schema)?;
            let path = args[1].evaluate(record, schema)?.to_string();

            if let Ok(json_value) = self.evaluate_json(json_input, path) {
                if json_value.is_object() || json_value.is_array() {
                    return Ok(Field::Json(json_value));
                }
                Ok(Field::Json(JsonValue::NULL))
            } else {
                Ok(Field::Null)
            }
        }
    }

    pub(crate) fn evaluate_json(
        &self,
        json_input: Field,
        path: String,
    ) -> Result<JsonValue, Error> {
        let json_val = match json_input.to_json() {
            Some(json) => json,
            None => JsonValue::NULL,
        };

        let finder = JsonPathFinder::new(
            Box::from(json_val),
            Box::from(JsonPathInst::from_str(path.as_str()).map_err(Error::InvalidJsonPath)?),
        );

        let found = finder.find();
        if let Some(a) = found.as_array() {
            if a.len() == 1 {
                return Ok(a.first().unwrap().clone());
            }
        }
        Ok(found)
    }
}
