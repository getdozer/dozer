use dozer_cache::cache::expression::{FilterExpression, Operator};
use dozer_types::{
    json_value_to_field,
    ordered_float::OrderedFloat,
    types::{Field, Schema},
};

use dozer_types::grpc_types::types::{value, EventType, Operation, OperationType, Record, Value};

pub fn op_satisfies_filter(
    op: &Operation,
    event_type: EventType,
    filter: Option<&FilterExpression>,
    schema: &Schema,
) -> bool {
    if let Some(filter) = filter {
        if (op.typ == OperationType::Insert as i32) || (op.typ == OperationType::Delete as i32) {
            if check_with_event_type(event_type, op) {
                record_satisfies_filter(op.new.as_ref().unwrap(), filter, schema)
            } else {
                false
            }
        } else if op.typ == OperationType::Update as i32 {
            if check_with_event_type(event_type, op) {
                record_satisfies_filter(op.old.as_ref().unwrap(), filter, schema)
                    || record_satisfies_filter(op.new.as_ref().unwrap(), filter, schema)
            } else {
                false
            }
        } else {
            false
        }
    } else {
        true
    }
}

fn check_with_event_type(event_type: EventType, op: &Operation) -> bool {
    if op.typ == OperationType::Insert as i32 {
        if (event_type == EventType::All) || (event_type == EventType::InsertOnly) {
            return true;
        }
    } else if op.typ == OperationType::Delete as i32 {
        if (event_type == EventType::All) || (event_type == EventType::DeleteOnly) {
            return true;
        }
    } else if op.typ == OperationType::Update as i32 {
        if (event_type == EventType::All) || (event_type == EventType::UpdateOnly) {
            return true;
        }
    } else {
        return false;
    }
    false
}

fn record_satisfies_filter(record: &Record, filter: &FilterExpression, schema: &Schema) -> bool {
    match filter {
        FilterExpression::And(filters) => filters
            .iter()
            .all(|filter| record_satisfies_filter(record, filter, schema)),
        FilterExpression::Simple(field_name, operator, value) => {
            let Some((field_index, field_definition)) = schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, field)| field.name == *field_name)
            else {
                return false;
            };

            let Some(filed_value) = record.values.get(field_index) else {
                return false;
            };

            let Ok(value) = json_value_to_field(
                value.clone(),
                field_definition.typ,
                field_definition.nullable,
            ) else {
                return false;
            };

            field_satisfies_op(filed_value, *operator, &value)
        }
    }
}

fn field_satisfies_op(field: &Value, operator: Operator, value: &Field) -> bool {
    match operator {
        Operator::LT => match (field.value.as_ref().unwrap(), value) {
            (value::Value::UintValue(n), Field::UInt(m)) => n < m,
            (value::Value::IntValue(n), Field::Int(m)) => n < m,
            (value::Value::FloatValue(n), Field::Float(m)) => &OrderedFloat(*n) < m,
            (value::Value::BoolValue(n), Field::Boolean(m)) => n < m,
            (value::Value::StringValue(n), Field::String(m)) => n < m,
            (value::Value::BytesValue(n), Field::Binary(m)) => n < m,
            _ => false,
        },
        Operator::LTE => match (field.value.as_ref().unwrap(), value) {
            (value::Value::UintValue(n), Field::UInt(m)) => n <= m,
            (value::Value::IntValue(n), Field::Int(m)) => n <= m,
            (value::Value::FloatValue(n), Field::Float(m)) => &OrderedFloat(*n) <= m,
            (value::Value::BoolValue(n), Field::Boolean(m)) => n <= m,
            (value::Value::StringValue(n), Field::String(m)) => n <= m,
            (value::Value::BytesValue(n), Field::Binary(m)) => n <= m,
            _ => false,
        },
        Operator::EQ => match (field.value.as_ref().unwrap(), value) {
            (value::Value::UintValue(n), Field::UInt(m)) => n == m,
            (value::Value::IntValue(n), Field::Int(m)) => n == m,
            (value::Value::FloatValue(n), Field::Float(m)) => &OrderedFloat(*n) == m,
            (value::Value::BoolValue(n), Field::Boolean(m)) => n == m,
            (value::Value::StringValue(n), Field::String(m)) => n == m,
            (value::Value::BytesValue(n), Field::Binary(m)) => n == m,
            _ => false,
        },
        Operator::GT => match (field.value.as_ref().unwrap(), value) {
            (value::Value::UintValue(n), Field::UInt(m)) => n > m,
            (value::Value::IntValue(n), Field::Int(m)) => n > m,
            (value::Value::FloatValue(n), Field::Float(m)) => &OrderedFloat(*n) > m,
            (value::Value::BoolValue(n), Field::Boolean(m)) => n > m,
            (value::Value::StringValue(n), Field::String(m)) => n > m,
            (value::Value::BytesValue(n), Field::Binary(m)) => n > m,
            _ => false,
        },
        Operator::GTE => match (field.value.as_ref().unwrap(), value) {
            (value::Value::UintValue(n), Field::UInt(m)) => n >= m,
            (value::Value::IntValue(n), Field::Int(m)) => n >= m,
            (value::Value::FloatValue(n), Field::Float(m)) => &OrderedFloat(*n) >= m,
            (value::Value::BoolValue(n), Field::Boolean(m)) => n >= m,
            (value::Value::StringValue(n), Field::String(m)) => n >= m,
            (value::Value::BytesValue(n), Field::Binary(m)) => n >= m,
            _ => false,
        },
        Operator::Contains => match (field.value.as_ref().unwrap(), value) {
            (value::Value::StringValue(n), Field::String(m)) => n.contains(m),
            _ => false,
        },
        Operator::MatchesAll | Operator::MatchesAny => unimplemented!(),
    }
}

#[cfg(test)]
mod tests;
