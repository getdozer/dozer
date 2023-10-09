use super::error::Error::{
    InputArgumentOverflow, OnnxInputDataMismatchErr, OnnxInvalidInputShapeErr,
    OnnxNotSupportedDataTypeErr, OnnxOrtErr, OnnxShapeErr,
};
use crate::error::Error::{self, Onnx};
use crate::execution::Expression;
use dozer_types::log::warn;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record, Schema};
use half::f16;
use ndarray::Array;
use num_traits::FromPrimitive;
use ort::tensor::TensorElementDataType;
use ort::{Session, Value};
use std::borrow::Borrow;
use std::ops::Deref;

pub fn evaluate_onnx_udf(
    schema: &Schema,
    session: &Session,
    args: &[Expression],
    record: &Record,
) -> Result<Field, Error> {
    let input_values = args
        .iter()
        .map(|arg| arg.evaluate(record, schema))
        .collect::<Result<Vec<_>, Error>>()?;

    let mut input_dim_prefix = false;
    let mut output_dim_prefix = false;

    let mut input_shape = vec![];
    for (i, d) in session.inputs[0].dimensions().enumerate() {
        if let Some(v) = d {
            input_shape.push(v);
        }
        if i == 0 && d.is_none() {
            input_dim_prefix = true;
        }
    }
    if input_shape.is_empty() {
        return Err(Onnx(OnnxInvalidInputShapeErr));
    }
    let mut output_shape = vec![];
    for (j, d) in session.inputs[0].dimensions().enumerate() {
        if let Some(v) = d {
            output_shape.push(v);
        }
        if j == 0 && d.is_none() {
            output_dim_prefix = true;
        }
    }
    if output_shape.is_empty() {
        return Err(Onnx(OnnxInvalidInputShapeErr));
    }
    let input_type = session.inputs[0].input_type;
    let return_type = session.outputs[0].output_type;

    if input_dim_prefix {
        input_shape.insert(0, 1);
    }
    if output_dim_prefix {
        output_shape.insert(0, 1);
    }

    match input_type {
        TensorElementDataType::Float32 => {
            warn!("Precision loss is expected due to conversion to f32");
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Float(v) = field {
                    let num = match f32::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Int(v) = field {
                    let num = match f32::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match f32::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::UInt(v) = field {
                    let num = match f32::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match f32::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            // assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Float64 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Float(v) = field {
                    input_array.push(*v);
                } else if let Field::Int(v) = field {
                    let num = match f64::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match f64::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::UInt(v) = field {
                    let num = match f64::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match f64::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Uint8 => {
            warn!("Precision loss is expected due to conversion to u8");
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    let num = match u8::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match u8::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match u8::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Int(v) = field {
                    let num = match u8::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match u8::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Uint16 => {
            let mut input_array = vec![];
            for field in input_values {
                warn!("Precision loss is expected due to conversion to u16");
                if let Field::UInt(v) = field {
                    let num = match u16::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match u16::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match u16::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Int(v) = field {
                    let num = match u16::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match u16::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Uint32 => {
            warn!("Precision loss is expected due to conversion to u32");
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    let num = match u32::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match u32::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match u32::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Int(v) = field {
                    let num = match u32::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match u32::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Uint64 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    input_array.push(v);
                } else if let Field::U128(v) = field {
                    let num = match u64::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match u64::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Int(v) = field {
                    let num = match u64::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match u64::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Int8 => {
            warn!("Precision loss is expected due to conversion to i8");
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    let num = match i8::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match i8::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::UInt(v) = field {
                    let num = match i8::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match i8::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match i8::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Int16 => {
            warn!("Precision loss is expected due to conversion to i16");
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    let num = match i16::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match i16::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::UInt(v) = field {
                    let num = match i16::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match i16::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match i16::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Int32 => {
            warn!("Precision loss is expected due to conversion to i32");
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    let num = match i32::from_i64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    let num = match i32::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::UInt(v) = field {
                    let num = match i32::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match i32::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match i32::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Int64 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    input_array.push(v);
                } else if let Field::I128(v) = field {
                    let num = match i64::from_i128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::UInt(v) = field {
                    let num = match i64::from_u64(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    let num = match i64::from_u128(v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else if let Field::Float(v) = field {
                    let num = match i64::from_f64(*v) {
                        Some(val) => val,
                        None => {
                            return Err(Onnx(InputArgumentOverflow(field.clone(), return_type)))
                        }
                    };
                    input_array.push(num);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::String => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::String(v) = field {
                    input_array.push(v);
                } else if let Field::Text(v) = field {
                    input_array.push(v);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        TensorElementDataType::Bool => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Boolean(v) = field {
                    input_array.push(v);
                } else {
                    return Err(Onnx(OnnxInputDataMismatchErr(input_type, field)));
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| Onnx(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array)
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session
                .run(input_tensor_values)
                .map_err(|e| Onnx(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape, output_dim_prefix)
        }
        _ => Err(Onnx(OnnxNotSupportedDataTypeErr(input_type))),
    }
}

fn onnx_output_to_dozer(
    return_type: TensorElementDataType,
    output: &Value,
    output_shape: Vec<usize>,
    output_dim_prefix: bool,
) -> Result<Field, Error> {
    if output_dim_prefix {
        match return_type {
            TensorElementDataType::Float16 => {
                let output_array_view = output
                    .try_extract::<f16>()
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?;
                assert_eq!(output_array_view.view().shape(), output_shape);
                let view = output_array_view.view();
                match view.deref().to_slice() {
                    Some(v) => {
                        let result = v[0].into();
                        Ok(Field::Float(OrderedFloat(result)))
                    }
                    None => Ok(Field::Null),
                }
            }
            TensorElementDataType::Float32 => {
                let output_array_view = output
                    .try_extract::<f32>()
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?;
                assert_eq!(output_array_view.view().shape(), output_shape);
                let view = output_array_view.view();
                match view.deref().to_slice() {
                    Some(v) => {
                        let result = v[0].into();
                        Ok(Field::Float(OrderedFloat(result)))
                    }
                    None => Ok(Field::Null),
                }
            }
            TensorElementDataType::Float64 => {
                let output_array_view = output
                    .try_extract::<f64>()
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?;
                assert_eq!(output_array_view.view().shape(), output_shape);
                let view = output_array_view.view();
                match view.deref().to_slice() {
                    Some(v) => {
                        let result = v[0];
                        Ok(Field::Float(OrderedFloat(result)))
                    }
                    None => Ok(Field::Null),
                }
            }
            _ => Err(Onnx(OnnxNotSupportedDataTypeErr(return_type))),
        }
    } else {
        match return_type {
            TensorElementDataType::Float16 => {
                let output_array_view = output
                    .try_extract::<f16>()
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?;
                assert_eq!(output_array_view.view().shape(), output_shape);
                Ok(Field::Float(OrderedFloat(
                    output_array_view.view().deref()[0].into(),
                )))
            }
            TensorElementDataType::Float32 => {
                let output_array_view = output
                    .try_extract::<f32>()
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?;
                assert_eq!(output_array_view.view().shape(), output_shape);
                let view = output_array_view.view();
                let result = view.deref()[0].into();
                Ok(Field::Float(OrderedFloat(result)))
            }
            TensorElementDataType::Float64 => {
                let output_array_view = output
                    .try_extract::<f64>()
                    .map_err(|e| Onnx(OnnxOrtErr(e)))?;
                assert_eq!(output_array_view.view().shape(), output_shape);
                Ok(Field::Float(OrderedFloat(
                    output_array_view.view().deref()[0],
                )))
            }
            _ => Err(Onnx(OnnxNotSupportedDataTypeErr(return_type))),
        }
    }
}
