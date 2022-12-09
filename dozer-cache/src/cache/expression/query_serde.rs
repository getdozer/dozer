use std::cmp::Ordering;
use std::collections::HashMap;

use dozer_types::serde::de::{self, Deserialize, Deserializer, Unexpected, Visitor};
use dozer_types::serde::ser::{self, Serialize, SerializeMap, Serializer};
use dozer_types::serde_json::Value;
use dozer_types::{serde, serde_json};

use crate::cache::expression::query_helper::{and_expression, simple_expression, sort_option};

use super::super::expression::FilterExpression;
use super::{Operator, SortOptions};

impl<'de> Deserialize<'de> for FilterExpression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterExpressionVisitor {}
        impl<'de> Visitor<'de> for FilterExpressionVisitor {
            type Value = FilterExpression;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("map from field name to value or operator value map")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut expressions = vec![];
                while let Some(key) = map.next_key::<String>()? {
                    let value: Value = map.next_value()?;

                    if key == "$and" {
                        let expression = and_expression(value)
                            .map_err(|err| de::Error::custom(err.to_string()))?;
                        expressions.push(expression);
                    } else {
                        let expression = simple_expression(key, value)
                            .map_err(|err| de::Error::custom(err.to_string()))?;
                        expressions.push(expression);
                    }
                }
                let size = expressions.len();
                match size.cmp(&1) {
                    Ordering::Equal => Ok(expressions[0].to_owned()),
                    Ordering::Greater => Ok(FilterExpression::And(expressions)),
                    Ordering::Less => Err(de::Error::invalid_value(
                        Unexpected::Str("No conditions specified"),
                        &self,
                    )),
                }
            }
        }
        deserializer.deserialize_map(FilterExpressionVisitor {})
    }
}

impl Serialize for FilterExpression {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FilterExpression::Simple(name, op, field_val) => {
                let mut state = serializer.serialize_map(Some(1))?;

                let val = match op {
                    Operator::EQ => field_val.to_owned(),
                    _ => {
                        let op_val = op.to_str();

                        let mut map = HashMap::new();
                        map.insert(op_val, field_val);
                        serde_json::to_value(map).map_err(|e| ser::Error::custom(e.to_string()))?
                    }
                };
                state.serialize_entry(name, &val)?;
                state.end()
            }
            FilterExpression::And(expressions) => {
                let mut state = serializer.serialize_map(Some(1))?;
                let value = serde_json::to_value(expressions)
                    .map_err(|e| ser::Error::custom(e.to_string()))?;

                state.serialize_entry("$and", &value)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SortOptions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SortOptionsVisitor {}
        impl<'de> Visitor<'de> for SortOptionsVisitor {
            type Value = SortOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("map from field name to sort direction")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut sort_options = vec![];
                while let Some(key) = map.next_key::<String>()? {
                    let value: Value = map.next_value()?;
                    let sort_option = sort_option(key, value)
                        .map_err(|err| de::Error::custom(err.to_string()))?;
                    sort_options.push(sort_option);
                }
                Ok(SortOptions(sort_options))
            }
        }
        deserializer.deserialize_map(SortOptionsVisitor {})
    }
}

impl Serialize for SortOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.0.len()))?;
        for sort_option in &self.0 {
            state.serialize_entry(&sort_option.field_name, sort_option.direction.to_str())?;
        }
        state.end()
    }
}
