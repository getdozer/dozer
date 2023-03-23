use std::sync::Arc;

use dozer_types::{
    arrow,
    chrono::Datelike,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};

fn field_type_to_arrow(field_type: FieldType) -> Option<arrow::datatypes::DataType> {
    match field_type {
        FieldType::UInt => Some(arrow::datatypes::DataType::UInt64),
        FieldType::Int => Some(arrow::datatypes::DataType::Int64),
        FieldType::Float => Some(arrow::datatypes::DataType::Float64),
        FieldType::Boolean => Some(arrow::datatypes::DataType::Boolean),
        FieldType::String => Some(arrow::datatypes::DataType::Utf8),
        FieldType::Text => Some(arrow::datatypes::DataType::LargeUtf8),
        FieldType::Binary => Some(arrow::datatypes::DataType::LargeBinary),
        FieldType::Decimal => None,
        FieldType::Timestamp => Some(arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        FieldType::Date => Some(arrow::datatypes::DataType::Date32),
        FieldType::Bson => None,
        FieldType::Point => None,
    }
}

fn field_definition_to_arrow(field_definition: FieldDefinition) -> Option<arrow::datatypes::Field> {
    field_type_to_arrow(field_definition.typ).map(|data_type| {
        arrow::datatypes::Field::new(field_definition.name, data_type, field_definition.nullable)
    })
}

pub fn schema_to_arrow(schema: Schema) -> (arrow::datatypes::Schema, Schema) {
    let arrow_fields = schema
        .fields
        .iter()
        .cloned()
        .filter_map(field_definition_to_arrow)
        .collect();
    let arrow_schema = arrow::datatypes::Schema::new(arrow_fields);

    let fields = schema
        .fields
        .into_iter()
        .filter_map(|field| field_type_to_arrow(field.typ).map(|_| field))
        .collect();
    let schema = Schema {
        identifier: schema.identifier,
        fields,
        primary_index: vec![],
    };

    (arrow_schema, schema)
}

fn fields_to_arrow<'a, F: IntoIterator<Item = Option<&'a Field>>>(
    fields: F,
    count: usize,
    field_type: FieldType,
) -> Option<Arc<dyn arrow::array::Array>> {
    Some(match field_type {
        FieldType::UInt => {
            let mut builder = arrow::array::UInt64Array::builder(count);
            for field in fields {
                match field? {
                    Field::UInt(value) => builder.append_value(*value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Int => {
            let mut builder = arrow::array::Int64Array::builder(count);
            for field in fields {
                match field? {
                    Field::Int(value) => builder.append_value(*value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Float => {
            let mut builder = arrow::array::Float64Array::builder(count);
            for field in fields {
                match field? {
                    Field::Float(value) => builder.append_value(value.0),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Boolean => {
            let mut builder = arrow::array::BooleanArray::builder(count);
            for field in fields {
                match field? {
                    Field::Boolean(value) => builder.append_value(*value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::String => {
            let mut builder = arrow::array::StringBuilder::new();
            for field in fields {
                match field? {
                    Field::String(value) => builder.append_value(value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Text => {
            let mut builder = arrow::array::LargeStringBuilder::new();
            for field in fields {
                match field? {
                    Field::Text(value) => builder.append_value(value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Binary => {
            let mut builder = arrow::array::LargeBinaryBuilder::new();
            for field in fields {
                match field? {
                    Field::Binary(value) => builder.append_value(value),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Decimal => panic!("Decimal not supported"),
        FieldType::Timestamp => {
            let mut builder = arrow::array::TimestampNanosecondArray::builder(count);
            for field in fields {
                match field? {
                    Field::Timestamp(value) => builder.append_value(value.timestamp_nanos()),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Date => {
            let mut builder = arrow::array::Date32Array::builder(count);
            for field in fields {
                match field? {
                    Field::Date(value) => builder.append_value(value.num_days_from_ce()),
                    Field::Null => builder.append_null(),
                    _ => panic!("Unexpected field type"),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Bson => panic!("Bson not supported"),
        FieldType::Point => panic!("Point not supported"),
    })
}

fn records_to_arrow<
    'a,
    I: Iterator<Item = Option<&'a Record>> + Clone,
    R: IntoIterator<IntoIter = I>,
>(
    records: R,
    count: usize,
    schema: Schema,
) -> Option<arrow::record_batch::RecordBatch> {
    let records = records.into_iter();

    let mut columns = vec![];
    for (index, field) in schema.fields.iter().enumerate() {
        if field_type_to_arrow(field.typ).is_some() {
            let fields = records
                .clone()
                .map(|record| record.map(|record| &record.values[index]));
            let column = fields_to_arrow(fields, count, field.typ);
            columns.push(column?);
        }
    }

    let (schema, _) = schema_to_arrow(schema);

    Some(
        arrow::record_batch::RecordBatch::try_new(Arc::new(schema), columns)
            .expect("BUG in records_to_arrow"),
    )
}

pub fn operations_to_arrow(
    operations: &[Operation],
    schema: Schema,
) -> Option<arrow::record_batch::RecordBatch> {
    let count = operations.len();
    let records = operations.iter().map(|operation| {
        if let Operation::Insert { new } = operation {
            Some(new)
        } else {
            None
        }
    });
    records_to_arrow(records, count, schema)
}
