use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{InvalidType, InvalidValue, UnsupportedSqlError};
use crate::pipeline::errors::UnsupportedSqlError::GenericError;
use crate::pipeline::expression::execution::Expression;
use dozer_core::daggy::Walker;
use dozer_types::json_types::JsonValue;
use dozer_types::log::warn;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::ort::sys::OrtTensorTypeAndShapeInfo;
use dozer_types::ort::tensor::TensorElementDataType;
use dozer_types::ort::tensor::TensorElementDataType::{
    Bool, Float16, Float32, Float64, Int16, Int32, Int64, String, Uint16, Uint32, Uint64,
};
use dozer_types::ort::value::DynArrayRef;
use dozer_types::ort::{Session, Value};
use dozer_types::serde_json::to_writer;
use dozer_types::types::{Field, FieldType, Record, Schema};
use image::Pixel;
use ndarray::{Array, CowArray, IxDyn};
use num_traits::{FromPrimitive, ToPrimitive};
use regex::bytes::Replacer;
use std::any::Any;
use std::borrow::Borrow;
use std::ops::{Add, Deref};
use std::path::Path;

pub fn evaluate_onnx_udf(
    schema: &Schema,
    session: &Session,
    args: &[Expression],
    return_type: &FieldType,
    record: &Record,
) -> Result<Field, PipelineError> {
    let input_values = args
        .iter()
        .map(|arg| arg.evaluate(record, schema))
        .collect::<Result<Vec<_>, PipelineError>>()?;

    let field = input_values[0].clone();
    match (field.clone(), return_type) {
        (Field::String(v), FieldType::String | FieldType::Json | FieldType::Float) => {
            let array =
                ndarray::CowArray::from(Array::from_shape_vec((1,), vec![v]).unwrap().into_dyn());
            let input_tensor_values = vec![Value::from_array(session.allocator(), &array).unwrap()];
            let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
            let output = outputs[0].borrow();

            match return_type {
                FieldType::String => {
                    let output_array_view = output.try_extract::<std::string::String>().unwrap();
                    Ok(Field::String(output_array_view.view().deref()[0].clone()))
                }
                FieldType::Json => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    let mut result = vec![];
                    for val in output_array_view.view().deref() {
                        result.push(JsonValue::Number(OrderedFloat(val.clone().into())));
                    }
                    Ok(Field::Json(JsonValue::Array(result)))
                }
                FieldType::Float => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    Ok(Field::Float(OrderedFloat(
                        output_array_view.view().deref()[0].clone().into(),
                    )))
                }
                _ => Err(InvalidValue(format!(
                    "{field} incompatible with {return_type}"
                ))),
            }
        }
        (Field::UInt(v), FieldType::Json | FieldType::Float | FieldType::UInt) => {
            let array =
                ndarray::CowArray::from(Array::from_shape_vec((1,), vec![v]).unwrap().into_dyn());
            let input_tensor_values = vec![Value::from_array(session.allocator(), &array).unwrap()];
            let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
            let output = outputs[0].borrow();

            match return_type {
                FieldType::Json => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    let mut result = vec![];
                    for val in output_array_view.view().deref() {
                        result.push(JsonValue::Number(OrderedFloat(val.clone().into())));
                    }
                    Ok(Field::Json(JsonValue::Array(result)))
                }
                FieldType::Float => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    Ok(Field::Float(OrderedFloat(
                        output_array_view.view().deref()[0].clone().into(),
                    )))
                }
                FieldType::UInt => {
                    let output_array_view = output.try_extract::<u32>().unwrap();
                    Ok(Field::UInt(
                        output_array_view.view().deref()[0].clone().into(),
                    ))
                }
                _ => Err(InvalidValue(format!(
                    "{field} incompatible with {return_type}"
                ))),
            }
        }
        (Field::U128(v), FieldType::Json | FieldType::Float | FieldType::U128) => {
            warn!("Precision loss happens due to conversion");
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(
                    (1,),
                    vec![u64::try_from(v)
                        .map_err(|e| InvalidType(field.clone(), return_type.to_string()))?],
                )
                .unwrap()
                .into_dyn(),
            );
            let input_tensor_values = vec![Value::from_array(session.allocator(), &array).unwrap()];
            let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
            let output = outputs[0].borrow();

            match return_type {
                FieldType::Json => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    let mut result = vec![];
                    for val in output_array_view.view().deref() {
                        result.push(JsonValue::Number(OrderedFloat(val.clone().into())));
                    }
                    Ok(Field::Json(JsonValue::Array(result)))
                }
                FieldType::Float => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    Ok(Field::Float(OrderedFloat(
                        output_array_view.view().deref()[0].clone().into(),
                    )))
                }
                FieldType::U128 => {
                    let output_array_view = output.try_extract::<u32>().unwrap();
                    Ok(Field::U128(
                        output_array_view.view().deref()[0].clone().into(),
                    ))
                }
                _ => Err(InvalidValue(format!(
                    "{field} incompatible with {return_type}"
                ))),
            }
        }
        (Field::Int(v), FieldType::Json | FieldType::Float | FieldType::Int) => {
            let array =
                ndarray::CowArray::from(Array::from_shape_vec((1,), vec![v]).unwrap().into_dyn());
            let input_tensor_values = vec![Value::from_array(session.allocator(), &array).unwrap()];
            let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
            let output = outputs[0].borrow();

            match return_type {
                FieldType::Json => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    let mut result = vec![];
                    for val in output_array_view.view().deref() {
                        result.push(JsonValue::Number(OrderedFloat(val.clone().into())));
                    }
                    Ok(Field::Json(JsonValue::Array(result)))
                }
                FieldType::Float => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    Ok(Field::Float(OrderedFloat(
                        output_array_view.view().deref()[0].clone().into(),
                    )))
                }
                FieldType::Int => {
                    let output_array_view = output.try_extract::<i32>().unwrap();
                    Ok(Field::Int(
                        output_array_view.view().deref()[0].clone().into(),
                    ))
                }
                _ => Err(InvalidValue(format!(
                    "{field} incompatible with {return_type}"
                ))),
            }
        }
        (Field::I128(v), FieldType::Json | FieldType::Float | FieldType::I128) => {
            warn!("Precision loss happens due to conversion");
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(
                    (1,),
                    vec![i64::try_from(v)
                        .map_err(|e| InvalidType(field.clone(), return_type.to_string()))?],
                )
                .unwrap()
                .into_dyn(),
            );
            let input_tensor_values = vec![Value::from_array(session.allocator(), &array).unwrap()];
            let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
            let output = outputs[0].borrow();

            match return_type {
                FieldType::Json => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    let mut result = vec![];
                    for val in output_array_view.view().deref() {
                        result.push(JsonValue::Number(OrderedFloat(val.clone().into())));
                    }
                    Ok(Field::Json(JsonValue::Array(result)))
                }
                FieldType::Float => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    Ok(Field::Float(OrderedFloat(
                        output_array_view.view().deref()[0].clone().into(),
                    )))
                }
                FieldType::I128 => {
                    let output_array_view = output.try_extract::<i32>().unwrap();
                    Ok(Field::I128(
                        output_array_view.view().deref()[0].clone().into(),
                    ))
                }
                _ => Err(InvalidValue(format!(
                    "{field} incompatible with {return_type}"
                ))),
            }
        }
        (Field::Float(v), FieldType::Json | FieldType::Float) => {
            warn!("Precision loss happens due to conversion");
            let num = match f32::from_f64(*v) {
                Some(val) => val,
                None => return Err(InvalidType(field.clone(), return_type.to_string())),
            };
            let array =
                ndarray::CowArray::from(Array::from_shape_vec((1,), vec![num]).unwrap().into_dyn());
            let input_tensor_values = vec![Value::from_array(session.allocator(), &array).unwrap()];
            let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
            let output = outputs[0].borrow();

            match return_type {
                FieldType::Json => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    let mut result = vec![];
                    for val in output_array_view.view().deref() {
                        result.push(JsonValue::Number(OrderedFloat(val.clone().into())));
                    }
                    Ok(Field::Json(JsonValue::Array(result)))
                }
                FieldType::Float => {
                    let output_array_view = output.try_extract::<f32>().unwrap();
                    Ok(Field::Float(OrderedFloat(
                        output_array_view.view().deref()[0].clone().into(),
                    )))
                }
                _ => Err(InvalidValue(format!(
                    "{field} incompatible with {return_type}"
                ))),
            }
        }
        (Field::Json(val), _) => match val {
            // JsonValue::Array(v) => {
            // },
            JsonValue::Number(v) => {
                warn!("Precision loss happens due to conversion");
                let num = match f32::from_f64(*v) {
                    Some(val) => val,
                    None => return Err(InvalidType(field.clone(), return_type.to_string())),
                };
                let array = ndarray::CowArray::from(
                    Array::from_shape_vec((1,), vec![num]).unwrap().into_dyn(),
                );
                let input_tensor_values =
                    vec![Value::from_array(session.allocator(), &array).unwrap()];
                let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
                let output = outputs[0].borrow();

                let output_array_view = output.try_extract::<f32>().unwrap();
                Ok(Field::Float(OrderedFloat(
                    output_array_view.view().deref()[0].clone().into(),
                )))
            }
            JsonValue::String(v) => {
                let array = ndarray::CowArray::from(
                    Array::from_shape_vec((1,), vec![v]).unwrap().into_dyn(),
                );
                let input_tensor_values =
                    vec![Value::from_array(session.allocator(), &array).unwrap()];
                let outputs: Vec<Value> = session.run(input_tensor_values).unwrap();
                let output = outputs[0].borrow();

                let output_array_view = output.try_extract::<std::string::String>().unwrap();
                Ok(Field::String(output_array_view.view().deref()[0].clone()))
            }
            _ => Err(InvalidValue(format!(
                "{field} incompatible with {return_type}"
            ))),
        },
        _ => Err(InvalidValue(format!(
            "{field} incompatible with {return_type}"
        ))),
    }
}
