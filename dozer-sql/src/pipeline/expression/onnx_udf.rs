use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::UnsupportedSqlError;
use crate::pipeline::errors::UnsupportedSqlError::GenericError;
use crate::pipeline::expression::execution::Expression;
use dozer_types::arrow::tensor::Tensor;
use dozer_types::crossbeam::epoch::Pointable;
use dozer_types::log::warn;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, Record, Schema};
use ort::download::language::GPT2;
use ort::sys::OrtTensorTypeAndShapeInfo;
use ort::tensor::TensorElementDataType::{
    Bool, Float16, Float32, Float64, Int16, Int32, Int64, String, Uint16, Uint32, Uint64,
};
use ort::tensor::{OrtOwnedTensor, TensorData, TensorElementDataType};
use ort::{
    Environment, ExecutionProvider, GraphOptimizationLevel, LoggingLevel, SessionBuilder, Value,
};
use sqlparser::tokenizer::Tokenizer;
use std::env;
use std::path::{Path, PathBuf};

const MODULE_NAME: &str = "onnx_udf";

pub fn evaluate_onnx_udf(
    schema: &Schema,
    name: &str,
    args: &[Expression],
    return_type: &FieldType,
    record: &Record,
) -> Result<Field, PipelineError> {
    let input_values = args
        .iter()
        .map(|arg| arg.evaluate(record, schema))
        .collect::<Result<Vec<_>, PipelineError>>()?;

    // dozer fields to ndarray

    let inputs = vec![Value::from_array(session.allocator(), &[])?];
    let outputs: Vec<Value> = session.run(inputs)?;

    // ort value to dozer fields
}

pub fn is_field_type_compatible(dozer_type: &FieldType, onnx_type: TensorElementDataType) -> bool {
    match (dozer_type, onnx_type) {
        (Float, f64 | f32 | f16) => {
            warn!("precision loss");
            true
        }
        (Int | I128, i64 | i32 | i16) => true,
        (UInt | U128, u64 | u32 | u16) => true,
        _ => false,
    }
}

pub fn map_onnx_type_to_dozer_type(onnx_type: TensorElementDataType) -> FieldType {
    match onnx_type {
        Float64 | Float32 | Float16 => FieldType::Float,
        Int64 | Int32 | Int16 => FieldType::Int,
        Uint64 | Uint32 | Uint16 => FieldType::UInt,
        String => FieldType::String,
        Bool => FieldType::Boolean,
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
