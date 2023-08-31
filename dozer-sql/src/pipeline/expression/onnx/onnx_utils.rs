use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::OnnxError;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::onnx::OnnxError::{
    ColumnNotFoundError, NonColumnArgFoundError, OnnxInputDataTypeMismatchErr, OnnxInputShapeErr,
    OnnxInputSizeErr, OnnxNotSupportedDataTypeErr, OnnxOutputShapeErr,
};
use dozer_types::arrow::datatypes::ArrowNativeTypeOp;
use dozer_types::types::{FieldType, Schema};
use ort::session::{Input, Output};
use ort::tensor::TensorElementDataType;

pub fn onnx_input_validation(
    schema: &Schema,
    args: &Vec<Expression>,
    inputs: &Vec<Input>,
) -> Result<(), PipelineError> {
    // 1. number of input & input shape check
    if inputs.len() != 1 {
        return Err(OnnxError(OnnxInputSizeErr(inputs.len())));
    }
    let mut flattened = 1_u32;
    let dim = inputs[0].dimensions.clone();
    for d in dim {
        match d {
            None => continue,
            Some(v) => {
                flattened = flattened.mul_wrapping(v);
            }
        }
    }
    if flattened as usize != args.len() || inputs.len() != 1 {
        return Err(OnnxError(OnnxInputShapeErr(flattened as usize, args.len())));
    }
    // 2. input datatype check
    for (input, arg) in inputs.iter().zip(args) {
        match arg {
            Expression::Column { index } => match schema.fields.get(*index) {
                Some(def) => match input.input_type {
                    TensorElementDataType::Float32 | TensorElementDataType::Float64 => {
                        if def.typ != FieldType::Float {
                            return Err(OnnxError(OnnxInputDataTypeMismatchErr(
                                input.input_type,
                                def.typ,
                            )));
                        }
                    }
                    TensorElementDataType::Uint8
                    | TensorElementDataType::Uint16
                    | TensorElementDataType::Uint32
                    | TensorElementDataType::Uint64 => {
                        if def.typ != FieldType::UInt && def.typ != FieldType::U128 {
                            return Err(OnnxError(OnnxInputDataTypeMismatchErr(
                                input.input_type,
                                def.typ,
                            )));
                        }
                    }
                    TensorElementDataType::Int8
                    | TensorElementDataType::Int16
                    | TensorElementDataType::Int32
                    | TensorElementDataType::Int64 => {
                        if def.typ != FieldType::Int && def.typ != FieldType::I128 {
                            return Err(OnnxError(OnnxInputDataTypeMismatchErr(
                                input.input_type,
                                def.typ,
                            )));
                        }
                    }
                    TensorElementDataType::String => {
                        if def.typ != FieldType::String && def.typ != FieldType::Text {
                            return Err(OnnxError(OnnxInputDataTypeMismatchErr(
                                input.input_type,
                                def.typ,
                            )));
                        }
                    }
                    TensorElementDataType::Bool => {
                        if def.typ != FieldType::Boolean {
                            return Err(OnnxError(OnnxInputDataTypeMismatchErr(
                                input.input_type,
                                def.typ,
                            )));
                        }
                    }
                    _ => return Err(OnnxError(OnnxNotSupportedDataTypeErr(input.input_type))),
                },
                None => return Err(OnnxError(ColumnNotFoundError(arg.clone()))),
            },
            _ => return Err(OnnxError(NonColumnArgFoundError(arg.clone()))),
        }
    }
    Ok(())
}

pub fn onnx_output_validation(outputs: &Vec<Output>) -> Result<(), PipelineError> {
    // 1. number of output & output shape check
    let mut flattened = 1_u32;
    for output_shape in outputs {
        let dim = output_shape.dimensions.clone();
        for d in dim {
            match d {
                None => continue,
                Some(v) => {
                    flattened = flattened.mul_wrapping(v);
                }
            }
        }
    }
    // output needs to be 1d single dim tensor
    if flattened as usize != 1_usize {
        return Err(OnnxError(OnnxOutputShapeErr(flattened as usize, 1_usize)));
    }
    // 2. output datatype check
    for output in outputs {
        match output.output_type {
            TensorElementDataType::Float32
            | TensorElementDataType::Float64
            | TensorElementDataType::Uint8
            | TensorElementDataType::Uint16
            | TensorElementDataType::Uint32
            | TensorElementDataType::Uint64
            | TensorElementDataType::Int8
            | TensorElementDataType::Int16
            | TensorElementDataType::Int32
            | TensorElementDataType::Int64
            | TensorElementDataType::String
            | TensorElementDataType::Bool => continue,
            _ => return Err(OnnxError(OnnxNotSupportedDataTypeErr(output.output_type))),
        }
    }
    Ok(())
}
