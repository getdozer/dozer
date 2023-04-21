use dozer_types::types::{Field, FieldDefinition, FieldType};

use crate::test_suite::{records::Operation, FieldsAndPk};

pub fn create_table_with_all_supported_data_types(table_name: &str) -> String {
    format!(
        r#"
        CREATE TABLE {table_name} (
            boolean BOOLEAN NOT NULL,
            boolean_null BOOLEAN, 
            int2 INT2 NOT NULL,
            int2_null INT2,
            int4 INT4 NOT NULL,
            int4_null INT4,
            int8 INT8 NOT NULL,
            int8_null INT8,
            char CHAR NOT NULL,
            char_null CHAR,
            text TEXT NOT NULL,
            text_null TEXT,
            varchar VARCHAR NOT NULL,
            varchar_null VARCHAR,
            bpchar BPCHAR NOT NULL,
            bpchar_null BPCHAR,
            float4 FLOAT4 NOT NULL,
            float4_null FLOAT4,
            float8 FLOAT8 NOT NULL,
            float8_null FLOAT8,
            bytea BYTEA NOT NULL,
            bytea_null BYTEA,
            timestamp TIMESTAMP NOT NULL,
            timestamp_null TIMESTAMP,
            timestamptz TIMESTAMPTZ NOT NULL,
            timestamptz_null TIMESTAMPTZ,
            numeric NUMERIC NOT NULL,
            numeric_null NUMERIC,
            jsonb JSONB NOT NULL,
            jsonb_null JSONB,
            date DATE NOT NULL,
            date_null DATE,
            point POINT NOT NULL,
            point_null POINT,
            uuid UUID
        );
        INSERT INTO {table_name} VALUES (
            false,
            false,
            0,
            0,
            0,
            0,
            0,
            0,
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            0.0,
            0.0,
            0.0,
            0.0,
            '',
            '',
            '1970-01-01 00:00:00',
            '1970-01-01 00:00:00',
            '1970-01-01 00:00:00',
            '1970-01-01 00:00:00',
            0,
            0,
            '{{}}'::json,
            '{{}}'::json,
            '1970-01-01',
            '1970-01-01',
            '(0,0)',
            '(0,0)',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID
        );
        INSERT INTO {table_name} VALUES (
            true,
            null,
            1,
            null,
            1,
            null,
            1,
            null,
            '1',
            null,
            '1',
            null,
            '1',
            null,
            '1',
            null,
            1.0,
            null,
            1.0,
            null,
            '1',
            null,
            '1970-01-01 00:00:00',
            null,
            '1970-01-01 00:00:00',
            null,
            1,
            null,
            '{{ "1": 1 }}'::json,
            null,
            '1970-01-01',
            null,
            '(1,1)',
            null,
            null
        );
        "#,
    )
}

pub fn create_schema(name: &str) -> String {
    format!(
        r#"
        CREATE SCHEMA {name};
        "#,
        name = name
    )
}

fn field_type_to_sql(field_type: FieldType) -> Option<String> {
    match field_type {
        FieldType::UInt => None,
        FieldType::U128 => None,
        FieldType::Int => Some("INT8".to_string()),
        FieldType::I128 => None,
        FieldType::Float => Some("FLOAT8".to_string()),
        FieldType::Boolean => Some("BOOLEAN".to_string()),
        FieldType::String => Some("TEXT".to_string()),
        FieldType::Text => None,
        FieldType::Binary => Some("BYTEA".to_string()),
        FieldType::Decimal => Some("NUMERIC".to_string()),
        FieldType::Timestamp => Some("TIMESTAMP".to_string()),
        FieldType::Date => Some("DATE".to_string()),
        FieldType::Bson => Some("JSONB".to_string()),
        FieldType::Point => Some("POINT".to_string()),
        FieldType::Duration => Some("DURATION".to_string()),
    }
}

fn field_definition_to_sql(field_definition: &FieldDefinition) -> Option<String> {
    let field_type = field_type_to_sql(field_definition.typ)?;
    let nullable = if field_definition.nullable {
        ""
    } else {
        " NOT NULL"
    };
    Some(format!(
        "{} {}{}",
        field_definition.name, field_type, nullable
    ))
}

pub fn schema_to_sql((fields, primary_index): FieldsAndPk) -> (FieldsAndPk, String) {
    let mut actual_fields = vec![];
    let mut actual_primary_index = vec![];

    let mut fields_sql = vec![];
    for (index, field) in fields.into_iter().enumerate() {
        let is_primary_key = primary_index.iter().any(|i| *i == index);

        let Some(mut field_sql) = field_definition_to_sql(&field) else {
            continue;
        };
        if is_primary_key {
            actual_primary_index.push(actual_fields.len());
            field_sql.push_str(" PRIMARY KEY");
        }
        actual_fields.push(field);
        fields_sql.push(field_sql);
    }
    ((actual_fields, actual_primary_index), fields_sql.join(", "))
}

fn full_table_name(schema_name: Option<&str>, table_name: &str) -> String {
    match schema_name {
        Some(schema_name) => format!("{}.{}", schema_name, table_name),
        None => table_name.to_string(),
    }
}

pub fn create_table(schema_name: Option<&str>, table_name: &str, schema: &FieldsAndPk) -> String {
    format!(
        r#"
        CREATE TABLE {table_name} ({fields});
        "#,
        table_name = full_table_name(schema_name, table_name),
        fields = schema_to_sql(schema.clone()).1
    )
}

fn field_to_sql(field: &Field) -> String {
    match field {
        Field::UInt(i) => i.to_string(),
        Field::U128(i) => i.to_string(),
        Field::Int(i) => i.to_string(),
        Field::I128(i) => i.to_string(),
        Field::Float(f) => f.to_string(),
        Field::Boolean(b) => b.to_string(),
        Field::String(s) => s.to_string(),
        Field::Text(s) => s.to_string(),
        Field::Binary(b) => format!("'\\x{}'", hex::encode(b)),
        Field::Decimal(d) => d.to_string(),
        Field::Timestamp(t) => format!("'{}'", t),
        Field::Date(d) => format!("'{}'", d),
        Field::Bson(b) => {
            let json = bson::from_slice::<dozer_types::serde_json::Value>(b).unwrap();
            format!("'{}'::json", json)
        }
        Field::Point(p) => format!("'({},{})'", p.0.x(), p.0.y()),
        Field::Duration(d) => d.to_string(),
        Field::Null => "NULL".to_string(),
    }
}

pub fn insert_record(
    schema_name: Option<&str>,
    table_name: &str,
    record: &[Field],
    fields: &[FieldDefinition],
) -> String {
    let mut values_sql = vec![];
    for (field, value) in fields.iter().zip(record.iter()) {
        if field_type_to_sql(field.typ).is_none() {
            continue;
        }
        values_sql.push(field_to_sql(value));
    }

    format!(
        r#"
        INSERT INTO {table_name} VALUES ({values});
        "#,
        table_name = full_table_name(schema_name, table_name),
        values = values_sql.join(", "),
    )
}

fn update_record(
    schema_name: Option<&str>,
    table_name: &str,
    old: &[Field],
    new: &[Field],
    (fields, primary_key): &FieldsAndPk,
) -> String {
    let mut set = vec![];
    let mut where_ = vec![];
    for (index, ((old_field, new_field), field_definition)) in
        old.iter().zip(new).zip(fields).enumerate()
    {
        if field_type_to_sql(field_definition.typ).is_none() {
            continue;
        }

        let is_primary_key = primary_key.iter().any(|i| *i == index);

        if is_primary_key {
            assert_eq!(old_field, new_field);
            where_.push(format!(
                "{} = {}",
                field_definition.name,
                field_to_sql(old_field)
            ));
        } else {
            set.push(format!(
                "{} = {}",
                field_definition.name,
                field_to_sql(new_field)
            ));
        }
    }

    format!(
        r#"
        UPDATE {table_name} SET {set} WHERE {where};
        "#,
        table_name = full_table_name(schema_name, table_name),
        set = set.join(", "),
        where = where_.join(" AND "),
    )
}

fn delete_record(
    schema_name: Option<&str>,
    table_name: &str,
    record: &[Field],
    (fields, primary_key): &FieldsAndPk,
) -> String {
    let mut where_ = vec![];
    for (index, (field, field_definition)) in record.iter().zip(fields).enumerate() {
        if field_type_to_sql(field_definition.typ).is_none() {
            continue;
        }

        let is_primary_key = primary_key.iter().any(|i| *i == index);

        if is_primary_key {
            where_.push(format!(
                "{} = {}",
                field_definition.name,
                field_to_sql(field)
            ));
        }
    }

    format!(
        r#"
        DELETE FROM {table_name} WHERE {where};
        "#,
        table_name = full_table_name(schema_name, table_name),
        where = where_.join(" AND "),
    )
}

pub fn operation_to_sql(
    schema_name: Option<&str>,
    table_name: &str,
    operation: &Operation,
    schema: &FieldsAndPk,
) -> String {
    match operation {
        Operation::Insert { new } => insert_record(schema_name, table_name, new, &schema.0),
        Operation::Update { new, old } => update_record(schema_name, table_name, old, new, schema),
        Operation::Delete { old } => delete_record(schema_name, table_name, old, schema),
    }
}
