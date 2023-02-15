use dozer_types::serde::{
    de::{Deserialize, Deserializer, MapAccess, Visitor},
    ser::{Serialize, SerializeMap, Serializer},
};

use crate::cache::expression::{query_helper::OperatorAndValue, SortOption};

use super::super::expression::FilterExpression;
use super::query_helper::OperatorAndValueBorrow;
use super::SortOptions;

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
                A: MapAccess<'de>,
            {
                let mut expressions = vec![];
                while let Some(key) = map.next_key::<String>()? {
                    if key == "$and" {
                        expressions.push(FilterExpression::And(map.next_value()?));
                    } else {
                        let operator_and_value = map.next_value::<OperatorAndValue>()?;
                        expressions.push(FilterExpression::Simple(
                            key,
                            operator_and_value.operator,
                            operator_and_value.value,
                        ));
                    }
                }
                if expressions.len() == 1 {
                    Ok(expressions.remove(0))
                } else {
                    Ok(FilterExpression::And(expressions))
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
            FilterExpression::Simple(name, operator, value) => {
                let mut state = serializer.serialize_map(Some(1))?;
                state.serialize_entry(name, &OperatorAndValueBorrow { operator, value })?;
                state.end()
            }
            FilterExpression::And(expressions) => {
                let mut state = serializer.serialize_map(Some(1))?;
                state.serialize_entry("$and", &expressions)?;
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
                A: MapAccess<'de>,
            {
                let mut sort_options = vec![];
                while let Some((field_name, direction)) = map.next_entry()? {
                    sort_options.push(SortOption {
                        field_name,
                        direction,
                    });
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
            state.serialize_entry(&sort_option.field_name, &sort_option.direction)?;
        }
        state.end()
    }
}
