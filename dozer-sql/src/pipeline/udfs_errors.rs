use crate::pipeline::expression::execution::Expression;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use ort::OrtError;
use ndarray::ShapeError;
use ort::tensor::TensorElementDataType;
use dozer_types::types::{Field, FieldType};

#[derive(Error, Debug)]
pub enum OnnxError {
    #[error("Onnx Ndarray Shape Error: {0}")]
    OnnxShapeErr(ShapeError),
    #[error("Onnx Runtime Error: {0}")]
    OnnxOrtErr(OrtError),
    #[error("Dozer expect onnx model to ingest single 1d input tensor: size of input {0}")]
    OnnxInputSizeErr(usize),
    #[error("Expected model input shape {0} doesn't match with actual input shape {1}")]
    OnnxInputShapeErr(usize, usize),
    #[error("Invalid input shape")]
    OnnxInvalidInputShapeErr,
    #[error("Expected model input datatype {0:?} doesn't match with actual input datatype {1}")]
    OnnxInputDataTypeMismatchErr(TensorElementDataType, FieldType),
    #[error("Expected model input datatype {0:?} doesn't match with actual input field {1}")]
    OnnxInputDataMismatchErr(TensorElementDataType, Field),
    #[error("Expected model output shape {0} doesn't match with actual output shape {1}")]
    OnnxOutputShapeErr(usize, usize),
    #[error("Dozer doesn't support following output datatype {0:?}")]
    OnnxNotSupportedDataTypeErr(TensorElementDataType),
    #[error("Dozer can't find following column in the input schema {0:?}")]
    ColumnNotFoundError(Expression),
    #[error("Dozer doesn't support non-column for onnx arguments {0:?}")]
    NonColumnArgFoundError(Expression),
}
