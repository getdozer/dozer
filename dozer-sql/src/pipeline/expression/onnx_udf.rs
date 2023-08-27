use std::borrow::Borrow;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::{InvalidType, InvalidValue, UnsupportedSqlError};
use crate::pipeline::errors::UnsupportedSqlError::GenericError;
use crate::pipeline::expression::execution::Expression;
use dozer_types::log::warn;
use dozer_types::types::{Field, FieldType, Record, Schema};
use dozer_types::ort::sys::OrtTensorTypeAndShapeInfo;
use dozer_types::ort::tensor::TensorElementDataType::{
    Bool, Float16, Float32, Float64, Int16, Int32, Int64, String, Uint16, Uint32, Uint64,
};
use dozer_types::ort::tensor::TensorElementDataType;
use dozer_types::ort::{Session, Value};
use std::path::Path;
use image::Pixel;
use ndarray::IxDyn;
use num_traits::{FromPrimitive, ToPrimitive};
use dozer_types::json_types::JsonValue;
use dozer_types::ort::value::DynArrayRef;

fn get_input_array(field: Field, return_type: FieldType) -> Result<DynArrayRef<'static>, PipelineError> {
    match (field, return_type) {
        (Field::UInt(v), FieldType::UInt) => Ok(DynArrayRef::Uint64(ndarray::CowArray::from(&[v]).into_dyn())),
        (Field::U128(v), FieldType::U128) => {
            warn!("Precision loss happens due to conversion");
            Ok(DynArrayRef::Uint64(ndarray::CowArray::from(&[u64::try_from(v)
                .map_err(|e| InvalidType(field.clone(), return_type.to_string()))?])
                .into_dyn()))
        },
        (Field::Int(v), FieldType::Int) => Ok(DynArrayRef::Int64(ndarray::CowArray::from(&[v]).into_dyn())),
        (Field::I128(v), FieldType::I128) => {
            warn!("Precision loss happens due to conversion");
            Ok(DynArrayRef::Int64(ndarray::CowArray::from(&[i64::try_from(v)
                .map_err(|e| InvalidType(field.clone(), return_type.to_string()))?])
                .into_dyn()))
        },
        (Field::Float(v), FieldType::Float) => {
            warn!("Precision loss happens due to conversion");
            let num = match f32::from_f64(*v) {
                Some(val) => val,
                None => return Err(InvalidType(field.clone(), return_type.to_string())),
            };
            Ok(DynArrayRef::Float(ndarray::CowArray::from(&[num]).into_dyn()))
        },
        (Field::Json(val), FieldType::Json) => match val {
            JsonValue::Array(v) => {
                for value in v {
                    let res = get_input_array(Field::Json(value), FieldType::Json)?;

                }
            },
            JsonValue::Number(v) => {
                warn!("Precision loss happens due to conversion");
                let num = match f32::from_f64(*v) {
                    Some(val) => val,
                    None => return Err(InvalidType(field.clone(), return_type.to_string())),
                };
                Ok(DynArrayRef::Float(ndarray::CowArray::from(&[num]).into_dyn()))
            },
            _ => Err(InvalidValue(format!("{field} incompatible with {return_type}"))),
        }
        _ => Err(InvalidValue(format!("{field} incompatible with {return_type}"))),
    }
}

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

    // let tensor_shape = get_input_tensor_shape(input_values[0].clone());
    let input_array = get_input_array(input_values[0].clone(), *return_type)?;

    let inputs = vec![Value::from_array(session.allocator(), *input_array).unwrap()];
    let outputs: Vec<Value> = session.run(inputs).unwrap();

    // ort value to dozer fields
    Ok(Field::Null)
}

fn return_type_to_primitive<T>(return_type: FieldType) -> T {

}

// pub fn is_field_type_compatible(dozer_type: &FieldType, onnx_type: TensorElementDataType) -> bool {
//     match (dozer_type, onnx_type) {
//         (FieldType::Float, Float64 | Float32 | Float16) => {
//             warn!("precision loss");
//             true
//         }
//         (FieldType::Int | FieldType::I128, Int64 | Int32 | Int16) => true,
//         (FieldType::UInt | FieldType::U128, Uint64 | Uint32 | Uint16) => true,
//         _ => false,
//     }
// }
//
// pub fn map_onnx_type_to_dozer_type(onnx_type: TensorElementDataType) -> Result<FieldType, PipelineError> {
//     match onnx_type {
//         Float64 | Float32 | Float16 => Ok(FieldType::Float),
//         Int64 | Int32 | Int16 => Ok(FieldType::Int),
//         Uint64 | Uint32 | Uint16 => Ok(FieldType::UInt),
//         String => Ok(FieldType::String),
//         Bool => Ok(FieldType::Boolean),
//         _ => Err(UnsupportedSqlError(GenericError(
//             "Unsupported type for onnx udf".to_string(),
//         ))),
//     }
// }

fn get_input_tensor_shape(field: Field) -> IxDyn {
    match field {
        // Field::Boolean(_) => IxDyn(&[1]),
        Field::UInt(_) => IxDyn(&[1]),
        Field::Int(_) => IxDyn(&[1]),
        Field::Float(_) => IxDyn(&[1]),
        // Field::Decimal(_) => IxDyn(&[1]),
        // Field::U128(_) => IxDyn(&[1]),
        // Field::I128(_) => IxDyn(&[1]),
        // Field::Json(val) => get_json_array_shape(val),
        _ => IxDyn(&[0]),
    }
}

// fn get_json_array_shape(val: JsonValue) -> unit {
//     let mut res = ();
//     match val {
//         JsonValue::Array(v) => {
//             res.extend(v.len());
//             v.into_iter().for_each(|item| get_json_array_shape(item))
//         },
//         JsonValue::Number(v) => res.extend(1),
//         _ => return res,
//     }
// }
