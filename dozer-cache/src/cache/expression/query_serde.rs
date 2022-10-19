use std::cmp::Ordering;
use std::collections::HashMap;

use dozer_types::serde::de::{self, Deserialize, Deserializer, Unexpected, Visitor};
use dozer_types::serde::ser::{self, Serialize, SerializeMap, Serializer};
use dozer_types::serde_json::Value;
use dozer_types::{serde, serde_json};

use crate::cache::expression::query_helper::{and_expression, simple_expression};

use super::super::expression::FilterExpression;
use super::Operator;

impl<'de> Deserialize<'de> for Operator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "$lt" => Ok(Operator::LT),
            "$lte" => Ok(Operator::LTE),
            "$gt" => Ok(Operator::GT),
            "$gte" => Ok(Operator::GTE),
            "$eq" => Ok(Operator::EQ),
            "$contains" => Ok(Operator::Contains),
            "$matches_any" => Ok(Operator::MatchesAny),
            "$matches_all" => Ok(Operator::MatchesAll),
            op => Err(de::Error::custom(format!("operator not found:  {}", op))),
        }
    }
}

fn get_operator_string(op: &Operator) -> &'static str {
    match op {
        Operator::LT => "$lt",
        Operator::LTE => "$lte",
        Operator::EQ => "$eq",
        Operator::GT => "$gt",
        Operator::GTE => "$gte",
        Operator::Contains => "$contains",
        Operator::MatchesAny => "$matches_any",
        Operator::MatchesAll => "$matches_all",
    }
}
impl Serialize for Operator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(get_operator_string(self))
    }
}

impl<'de> Deserialize<'de> for FilterExpression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterExpressionVisitor {}
        impl<'de> Visitor<'de> for FilterExpressionVisitor {
            type Value = FilterExpression;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Could not deserialize FilterExpression")
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
                        let op_val = get_operator_string(op);

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
