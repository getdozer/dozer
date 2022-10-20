use super::query_helper::{is_combinator, value_to_composite_expression, value_to_simple_exp};
use dozer_types::serde_json::Value;
use serde::de::{self, Deserialize, Deserializer, Visitor};

use super::super::expression::FilterExpression;

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
                if let Some(key) = map.next_key::<String>()? {
                    let value: Value = map.next_value()?;
                    if !is_combinator(key.to_owned()) {
                        let expression = value_to_simple_exp(key, value)
                            .map_err(|err| de::Error::custom(err.to_string()))?;
                        return Ok(expression);
                    } else {
                        let composite_expression = value_to_composite_expression(key, value)
                            .map_err(|err| de::Error::custom(err.to_string()))?;
                        return Ok(composite_expression);
                    }
                }
                Err(de::Error::unknown_variant(
                    "Cannot regconized as FilterExpression",
                    &["Simple", "And"],
                ))
            }
        }
        deserializer.deserialize_map(FilterExpressionVisitor {})
    }
}
