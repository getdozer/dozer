use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{InvalidType, InvalidValue, OnnxOrtErr, OnnxShapeErr};
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

    let input_shape: Vec<usize> = session.inputs[0].dimensions().map(|d| d.unwrap()).collect();
    let output_shape: Vec<usize> = session.outputs[0].dimensions().map(|d| d.unwrap()).collect();
    let return_type = session.outputs[0].output_type;

    match input_values[0].clone() {
        Field::String(_) => {
            let mut input_array = vec![];
            for field in input_values {
                match field {
                    Field::String(val) => input_array.push(val),
                    _ => return Err(InvalidValue(format!("Field {field} is incompatible input"))),
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape, input_array)
                    .map_err(OnnxShapeErr)?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(OnnxOrtErr)?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(OnnxOrtErr)?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);

            match return_type {
                TensorElementDataType::Float16 => {
                    let output_array_view = output.try_extract::<f16>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float32 => {
                    let output_array_view = output.try_extract::<f32>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float64 => {
                    let output_array_view = output.try_extract::<f64>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                _ => Err(InvalidValue(format!(
                    "{return_type:?} is incompatible return type"
                ))),
            }
        }
        Field::UInt(_) => {
            warn!("Precision loss happens due to conversion from u64 to u32");
            let mut input_array = vec![];
            for field in input_values {
                match field {
                    Field::UInt(val) => input_array.push(u32::try_from(val)
                        .map_err(|_e| InvalidType(field.clone(), format!("{:?}", return_type)))?),
                    _ => return Err(InvalidValue(format!("Field {field} is incompatible input"))),
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape, input_array)
                    .map_err(OnnxShapeErr)?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(OnnxOrtErr)?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(OnnxOrtErr)?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);

            match return_type {
                TensorElementDataType::Float16 => {
                    let output_array_view = output.try_extract::<f16>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float32 => {
                    let output_array_view = output.try_extract::<f32>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float64 => {
                    let output_array_view = output.try_extract::<f64>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                _ => Err(InvalidValue(format!(
                    "{return_type:?} is incompatible return type"
                ))),
            }
        }
        Field::U128(_) => {
            warn!("Precision loss happens due to conversion from u128 to u32");
            let mut input_array = vec![];
            for field in input_values {
                match field {
                    Field::U128(val) => input_array.push(u32::try_from(val)
                        .map_err(|_e| InvalidType(field.clone(), format!("{:?}", return_type)))?),
                    _ => return Err(InvalidValue(format!("Field {field} is incompatible input"))),
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape, input_array)
                    .map_err(OnnxShapeErr)?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(OnnxOrtErr)?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(OnnxOrtErr)?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);

            match return_type {
                TensorElementDataType::Float16 => {
                    let output_array_view = output.try_extract::<f16>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float32 => {
                    let output_array_view = output.try_extract::<f32>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float64 => {
                    let output_array_view = output.try_extract::<f64>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                _ => Err(InvalidValue(format!(
                    "{return_type:?} is incompatible return type"
                ))),
            }
        }
        Field::Int(_) => {
            warn!("Precision loss happens due to conversion from i64 to u32");
            let mut input_array = vec![];
            for field in input_values {
                match field {
                    Field::Int(val) => input_array.push(i32::try_from(val)
                        .map_err(|_e| InvalidType(field.clone(), format!("{:?}", return_type)))?),
                    _ => return Err(InvalidValue(format!("Field {field} is incompatible input"))),
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape, input_array)
                    .map_err(OnnxShapeErr)?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(OnnxOrtErr)?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(OnnxOrtErr)?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);

            match return_type {
                TensorElementDataType::Float16 => {
                    let output_array_view = output.try_extract::<f16>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float32 => {
                    let output_array_view = output.try_extract::<f32>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float64 => {
                    let output_array_view = output.try_extract::<f64>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                _ => Err(InvalidValue(format!(
                    "{return_type:?} is incompatible return type"
                ))),
            }
        }
        Field::I128(_) => {
            warn!("Precision loss happens due to conversion from i128 to i32");
            let mut input_array = vec![];
            for field in input_values {
                match field {
                    Field::I128(val) => input_array.push(i32::try_from(val)
                        .map_err(|_e| InvalidType(field.clone(), format!("{:?}", return_type)))?),
                    _ => return Err(InvalidValue(format!("Field {field} is incompatible input"))),
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape, input_array)
                    .map_err(OnnxShapeErr)?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(OnnxOrtErr)?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(OnnxOrtErr)?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);

            match return_type {
                TensorElementDataType::Float16 => {
                    let output_array_view = output.try_extract::<f16>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float32 => {
                    let output_array_view = output.try_extract::<f32>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float64 => {
                    let output_array_view = output.try_extract::<f64>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                _ => Err(InvalidValue(format!(
                    "{return_type:?} is incompatible return type"
                ))),
            }
        }
        Field::Float(_) => {
            warn!("Precision loss happens due to conversion from f64 to f32");
            let mut input_array = vec![];
            for field in input_values {
                match field {
                    Field::Float(val) => {
                        let num = match f32::from_f64(*val) {
                            Some(val) => val,
                            None => return Err(InvalidType(field.clone(), format!("{:?}", return_type))),
                        };
                        input_array.push(num)
                    },
                    _ => return Err(InvalidValue(format!("Field {field} is incompatible input"))),
                }
            }
            let array = ndarray::CowArray::from(
                Array::from_shape_vec(input_shape, input_array)
                    .map_err(OnnxShapeErr)?
                    .into_dyn(),
            );
            let input_tensor_values =
                vec![Value::from_array(session.allocator(), &array).map_err(OnnxOrtErr)?];
            let outputs: Vec<Value> = session.run(input_tensor_values).map_err(OnnxOrtErr)?;
            let output = outputs[0].borrow();

            // number of output validation
            assert_eq!(outputs.len(), 1);

            match return_type {
                TensorElementDataType::Float16 => {
                    let output_array_view = output.try_extract::<f16>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                TensorElementDataType::Float32 => {
                    let output_array_view = output.try_extract::<f32>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    let result = output_array_view.view().deref()[0].into();
                    Ok(Field::Float(OrderedFloat(result)))
                }
                TensorElementDataType::Float64 => {
                    let output_array_view = output.try_extract::<f64>().map_err(OnnxOrtErr)?;
                    assert_eq!(output_array_view.view().shape(), output_shape);
                    Ok(Field::Float(OrderedFloat(output_array_view.view().deref()[0].into())))
                }
                _ => Err(InvalidValue(format!(
                    "{return_type:?} is incompatible return type"
                ))),
            }
        }
        _ => Err(InvalidValue(format!(
            "{:?} is incompatible input type", input_values[0]
        ))),
    }
}
