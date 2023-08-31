use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{InvalidType, OnnxError};
use crate::pipeline::expression::execution::Expression;
use dozer_types::log::warn;
use dozer_types::ordered_float::OrderedFloat;
use ort::{Session, Value};
use dozer_types::types::{Field, Record, Schema};
use ndarray::Array;
use num_traits::FromPrimitive;
use std::borrow::Borrow;
use std::ops::Deref;
use half::f16;
use ort::tensor::TensorElementDataType;
use crate::pipeline::udfs_errors::OnnxError::{OnnxInputDataMismatchErr, OnnxInvalidInputShapeErr, OnnxNotSupportedDataTypeErr, OnnxOrtErr, OnnxShapeErr};

pub fn evaluate_onnx_udf(
    schema: &Schema,
    session: &Session,
    args: &[Expression],
    record: &Record,
) -> Result<Field, PipelineError> {
    let input_values = args
        .iter()
        .map(|arg| arg.evaluate(record, schema))
        .collect::<Result<Vec<_>, PipelineError>>()?;

    let mut input_shape = vec![];
    for d in session.inputs[0].dimensions() {
        match d {
            Some(v) => input_shape.push(v),
            None => return Err(OnnxError(OnnxInvalidInputShapeErr)),
        }
    }
    let mut output_shape = vec![];
    for d in session.outputs[0].dimensions() {
        match d {
            Some(v) => output_shape.push(v),
            None => return Err(OnnxError(OnnxInvalidInputShapeErr)),
        }
    }
    let input_type = session.inputs[0].input_type;
    let return_type = session.outputs[0].output_type;

    match input_type {
        TensorElementDataType::Float32 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Float(v) = field {
                    warn!("Precision loss happens due to conversion from f64 to f32");
                    let num = match f32::from_f64(*v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Float64 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Float(v) = field {
                    input_array.push(*v);
                } else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Uint8 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    warn!("Precision loss happens due to conversion from u64 to u8");
                    let num = match u8::from_u64(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    warn!("Precision loss happens due to conversion from u128 to u8");
                    let num = match u8::from_u128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Uint16 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    warn!("Precision loss happens due to conversion from u64 to u16");
                    let num = match u16::from_u64(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    warn!("Precision loss happens due to conversion from u128 to u16");
                    let num = match u16::from_u128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Uint32 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    warn!("Precision loss happens due to conversion from u64 to u32");
                    let num = match u32::from_u64(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else if let Field::U128(v) = field {
                    warn!("Precision loss happens due to conversion from u128 to u32");
                    let num = match u32::from_u128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Uint64 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::UInt(v) = field {
                    input_array.push(v);
                } else if let Field::U128(v) = field {
                    warn!("Precision loss happens due to conversion from u128 to u64");
                    let num = match u64::from_u128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Int8 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    warn!("Precision loss happens due to conversion from i64 to i8");
                    let num = match i8::from_i64(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    warn!("Precision loss happens due to conversion from i128 to i8");
                    let num = match i8::from_i128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Int16 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    warn!("Precision loss happens due to conversion from i64 to i16");
                    let num = match i16::from_i64(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    warn!("Precision loss happens due to conversion from i128 to i16");
                    let num = match i16::from_i128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Int32 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    warn!("Precision loss happens due to conversion from i64 to i32");
                    let num = match i32::from_i64(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                } else if let Field::I128(v) = field {
                    warn!("Precision loss happens due to conversion from i128 to i32");
                    let num = match i32::from_i128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        }
        TensorElementDataType::Int64 => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Int(v) = field {
                    input_array.push(v);
                } else if let Field::I128(v) = field {
                    warn!("Precision loss happens due to conversion from i128 to i64");
                    let num = match i64::from_i128(v) {
                        Some(val) => val,
                        None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                    };
                    input_array.push(num);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::String => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::String(v) = field {
                    input_array.push(v);
                } else if let Field::Text(v) = field {
                    input_array.push(v);
                }
                else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        TensorElementDataType::Bool => {
            let mut input_array = vec![];
            for field in input_values {
                if let Field::Boolean(v) = field {
                    input_array.push(v);
                } else {
                    return Err(OnnxError(OnnxInputDataMismatchErr(input_type, field)))
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape.clone(), input_array)
                    .map_err(|e| OnnxError(OnnxShapeErr(e)))?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(|e| OnnxError(OnnxOrtErr(e)))?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);
            onnx_output_to_dozer(return_type, output, output_shape)
        },
        _ => return Err(OnnxError(OnnxNotSupportedDataTypeErr(input_type)))
    }
}

fn onnx_output_to_dozer(return_type: TensorElementDataType, output: &Value, output_shape: Vec<usize>) -> Result<Field, PipelineError> {
    match return_type {
        TensorElementDataType::Float16 => {
            let output_array_view = output.try_extract::<f16>().map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            assert_eq!(output_array_view.view().shape(), output_shape);
            Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
        }
        TensorElementDataType::Float32 => {
            let output_array_view = output.try_extract::<f32>().map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            assert_eq!(output_array_view.view().shape(), output_shape);
            let result = output_array_view.view().deref()[0].into();
            Ok(Field::Float(OrderedFloat(result)))
        }
        TensorElementDataType::Float64 => {
            let output_array_view = output.try_extract::<f64>().map_err(|e| OnnxError(OnnxOrtErr(e)))?;
            assert_eq!(output_array_view.view().shape(), output_shape);
            Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
        }
        _ => Err(OnnxError(OnnxNotSupportedDataTypeErr(return_type)))
    }
}
