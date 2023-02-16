use dozer_types::serde::{
    de::{self, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize,
};
use dozer_types::serde_json::Value;

use super::super::expression::Operator;

pub struct OperatorAndValue {
    pub operator: Operator,
    pub value: Value,
}

impl<'de> Deserialize<'de> for OperatorAndValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OperatorAndValueVisitor {}
        impl<'de> Visitor<'de> for OperatorAndValueVisitor {
            type Value = OperatorAndValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("value or map from operator to value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::Bool(v),
                })
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::String(v.to_string()),
                })
            }

            fn visit_char<E>(self, v: char) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::String(v.to_string()),
                })
            }

            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                if let Some((operator, value)) = map.next_entry()? {
                    if map.next_entry::<Operator, Value>()?.is_some() {
                        Err(de::Error::custom(
                            "More than one statement passed in Simple Expression",
                        ))
                    } else {
                        Ok(OperatorAndValue { operator, value })
                    }
                } else {
                    Err(de::Error::custom("empty object passed as value"))
                }
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::Null,
                })
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::String(v.to_string()),
                })
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::String(v),
                })
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::from(v),
                })
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OperatorAndValue {
                    operator: Operator::EQ,
                    value: Value::Null,
                })
            }
        }
        deserializer.deserialize_any(OperatorAndValueVisitor {})
    }
}

pub struct OperatorAndValueBorrow<'a> {
    pub operator: &'a Operator,
    pub value: &'a Value,
}

impl<'a> Serialize for OperatorAndValueBorrow<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: dozer_types::serde::Serializer,
    {
        match self.operator {
            Operator::EQ => self.value.serialize(serializer),
            _ => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(self.operator, self.value)?;
                map.end()
            }
        }
    }
}
