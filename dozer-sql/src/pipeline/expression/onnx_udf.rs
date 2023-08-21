use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::UnsupportedSqlError;
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

    let image_buffer = image::open(Path::new("/Users/chloeminkyung/CLionProjects/dozer/dozer-sql/src/pipeline/expression/tests/models/mushroom.png"))
        .unwrap()
        .to_rgb8();

    // dozer fields to ndarray
    let array = ndarray::CowArray::from(
        ndarray::Array::from_shape_fn((1, 224, 224, 3), |(_, j, i, c)| {
            let pixel = image_buffer.get_pixel(i as u32, j as u32);
            let channels = pixel.channels();

            // range [0, 255] -> range [0, 1]
            (channels[c] as f32) / 255.0
        })
        .into_dyn(),
    );

    let inputs = vec![Value::from_array(session.allocator(),&array).unwrap()];
    let outputs: Vec<Value> = session.run(inputs).unwrap();

    // ort value to dozer fields
    Ok(Field::Null)
}

pub fn is_field_type_compatible(dozer_type: &FieldType, onnx_type: TensorElementDataType) -> bool {
    match (dozer_type, onnx_type) {
        (FieldType::Float, Float64 | Float32 | Float16) => {
            warn!("precision loss");
            true
        }
        (FieldType::Int | FieldType::I128, Int64 | Int32 | Int16) => true,
        (FieldType::UInt | FieldType::U128, Uint64 | Uint32 | Uint16) => true,
        _ => false,
    }
}

pub fn map_onnx_type_to_dozer_type(onnx_type: TensorElementDataType) -> Result<FieldType, PipelineError> {
    match onnx_type {
        Float64 | Float32 | Float16 => Ok(FieldType::Float),
        Int64 | Int32 | Int16 => Ok(FieldType::Int),
        Uint64 | Uint32 | Uint16 => Ok(FieldType::UInt),
        String => Ok(FieldType::String),
        Bool => Ok(FieldType::Boolean),
        _ => Err(UnsupportedSqlError(GenericError(
            "Unsupported type for onnx udf".to_string(),
        ))),
    }
}

pub fn convert_dozer_field_to_onnx_tensor(
    fields: &[Field],
    onnx_type: TensorElementDataType,
    tensor_shape: OrtTensorTypeAndShapeInfo,
) {
    todo!();
    // let mut result = OrtTensor::new();
    // for field in fields {
    //     match (field, onnx_type) {
    //         (Field::UInt(val), Uint16) => {
    //             tensor.set
    //         }
    //     }
    // }
}
