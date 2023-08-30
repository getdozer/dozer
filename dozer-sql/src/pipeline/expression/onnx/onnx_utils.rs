use ort::session::{Input, Output};
use ort::tensor::TensorElementDataType;
use dozer_types::arrow::datatypes::ArrowNativeTypeOp;
use dozer_types::types::{FieldType, Schema};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;

fn onnx_input_validation(schema: &Schema, args: Vec<Expression>, inputs: &Vec<Input>) -> Result<(), PipelineError> {
    // 1. number of input & input shape check
    if inputs.len() != 1 {
        return Err(OnnxValidationErr("Dozer expect onnx model to ingest single 1d input tensor".to_string()))
    }
    let mut flattened = 1_u32;
    let dim = inputs[0].dimensions.clone();
    for d in dim {
        match d {
            None => continue,
            Some(v) => {
                flattened = flattened.mul_wrapping(v);
            },
        }
    }
    if flattened as usize != args.len() || inputs.len() != 1 {
        return Err(OnnxValidationErr(format!("Expected model input shape {} doesn't match with actual input shape {}", flattened, args.len())))
    }
    // 2. input datatype check
    for (input, arg) in inputs.into_iter().zip(args) {
        match arg {
            Expression::Column {index} => {
                match schema.fields.get(index) {
                    Some(def) => {
                        match input.input_type {
                            TensorElementDataType::Float32
                            | TensorElementDataType::Float64 => {
                                if def.typ != FieldType::Float {
                                    return Err(OnnxValidationErr(
                                        format!("Expected model input datatype {:?} doesn't match with actual input datatype {}",
                                                input.input_type, def.typ)
                                    ))
                                }
                            },
                            TensorElementDataType::Uint8
                            | TensorElementDataType::Uint16
                            | TensorElementDataType::Uint32
                            | TensorElementDataType::Uint64 => {
                                if def.typ != FieldType::UInt && def.typ != FieldType::U128 {
                                    return Err(OnnxValidationErr(
                                        format!("Expected model input datatype {:?} doesn't match with actual input datatype {}",
                                                input.input_type, def.typ)
                                    ))
                                }
                            },
                            TensorElementDataType::Int8
                            | TensorElementDataType::Int16
                            | TensorElementDataType::Int32
                            | TensorElementDataType::Int64 => {
                                if def.typ != FieldType::Int && def.typ != FieldType::I128 {
                                    return Err(OnnxValidationErr(
                                        format!("Expected model input datatype {:?} doesn't match with actual input datatype {}",
                                                input.input_type, def.typ)
                                    ))
                                }
                            },
                            TensorElementDataType::String => {
                                if def.typ != FieldType::String && def.typ != FieldType::Text {
                                    return Err(OnnxValidationErr(
                                        format!("Expected model input datatype {:?} doesn't match with actual input datatype {}",
                                                input.input_type, def.typ)
                                    ))
                                }
                            },
                            TensorElementDataType::Bool => {
                                if def.typ != FieldType::Boolean {
                                    return Err(OnnxValidationErr(
                                        format!("Expected model input datatype {:?} doesn't match with actual input datatype {}",
                                                input.input_type, def.typ)
                                    ))
                                }
                            },
                            _ => return Err(OnnxValidationErr(
                                format!("Dozer doesn't support following input datatype {:?}", input.input_type)
                            ))
                        }
                    },
                    None => return Err(OnnxValidationErr(
                        format!("Dozer can't find following column in the input schema {:?}", arg)
                    ))
                }
            }
            _ => return Err(OnnxValidationErr(
                format!("Dozer doesn't support non-column for onnx arguments {:?}", arg)
            ))
        }
    }
    Ok(())
}

fn onnx_output_validation(outputs: &Vec<Output>) -> Result<(), PipelineError> {
    // 1. number of output & output shape check
    let mut flattened = 1_u32;
    for output_shape in outputs {
        let dim = output_shape.dimensions.clone();
        for d in dim {
            match d {
                None => continue,
                Some(v) => {
                    flattened = flattened.mul_wrapping(v);
                },
            }
        }
    }
    // output needs to be 1d single dim tensor
    // if flattened as usize != 1_usize {
    //     return Err(OnnxValidationErr(format!("Expected model output shape {} doesn't match with actual output shape {}", flattened, 1_usize)))
    // }
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
            | TensorElementDataType::Bool => {
                continue
            },
            _ => return Err(OnnxValidationErr(
                format!("Dozer doesn't support following output datatype {:?}", output.output_type)
            ))
        }
    }
    Ok(())
}
