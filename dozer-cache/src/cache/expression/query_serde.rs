use dozer_types::serde::{
    de::{self, Deserialize, Deserializer, Error, MapAccess, Visitor},
    ser::{Serialize, SerializeMap, Serializer},
};

use super::{
    super::expression::{FilterExpression, Skip, SortOption},
    query_helper::{OperatorAndValue, OperatorAndValueBorrow},
    QueryExpression, SQLQuery, SortOptions,
};

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

impl<'de> Deserialize<'de> for QueryExpression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct QueryExpressionVisitor {}
        impl<'de> Visitor<'de> for QueryExpressionVisitor {
            type Value = QueryExpression;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("map of dozer query options")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut filter = None;
                let mut order_by = None;
                let mut limit = None;
                let mut skip = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "$filter" => {
                            filter = Some(map.next_value()?);
                        }
                        "$order_by" => {
                            order_by = Some(map.next_value()?);
                        }
                        "$limit" => {
                            limit = Some(map.next_value()?);
                        }
                        "$skip" => {
                            if skip.is_some() {
                                return Err(Error::custom("$skip cannot be used with $after"));
                            }
                            skip = Some(Skip::Skip(map.next_value()?));
                        }
                        "$after" => {
                            if skip.is_some() {
                                return Err(Error::custom("$after cannot be used with $skip"));
                            }
                            skip = Some(Skip::After(map.next_value()?));
                        }
                        _ => {}
                    }
                }
                Ok(QueryExpression {
                    filter,
                    order_by: order_by.unwrap_or_default(),
                    limit,
                    skip: skip.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_map(QueryExpressionVisitor {})
    }
}

impl Serialize for QueryExpression {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(4))?;
        if let Some(filter) = &self.filter {
            state.serialize_entry("$filter", filter)?;
        }
        if !self.order_by.0.is_empty() {
            state.serialize_entry("$order_by", &self.order_by)?;
        }
        if let Some(limit) = self.limit {
            state.serialize_entry("$limit", &limit)?;
        }
        match self.skip {
            Skip::Skip(skip) => {
                if skip > 0 {
                    state.serialize_entry("$skip", &skip)?;
                }
            }
            Skip::After(after) => {
                state.serialize_entry("$after", &after)?;
            }
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for SQLQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SQLQueryVisitor;
        impl<'de> Visitor<'de> for SQLQueryVisitor {
            type Value = SQLQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("map containing a single key 'query'")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut query = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key.as_str() == "query" {
                        query = Some(map.next_value()?);
                        break;
                    }
                }
                if let Some(query) = query {
                    Ok(SQLQuery(query))
                } else {
                    Err(de::Error::missing_field("query"))
                }
            }
        }
        deserializer.deserialize_map(SQLQueryVisitor)
    }
}

impl Serialize for SQLQuery {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(1))?;
        state.serialize_entry("query", &self.0)?;
        state.end()
    }
}
