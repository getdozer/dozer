use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::UnsupportedSqlError;
use crate::pipeline::errors::UnsupportedSqlError::GenericError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, Record, Schema};
use pyo3::types::PyTuple;
use pyo3::Python;

pub fn evaluate_py_udf(
    schema: &Schema,
    name: &str,
    args: &[Expression],
    return_type: &FieldType,
    record: &Record,
) -> Result<Field, PipelineError> {
    let mut values = vec![];
    for arg in args {
        values.push(arg.evaluate(record, schema)?)
    }
    Python::with_gil(|py| -> Result<Field, PipelineError> {
        let module = py.import("python_udf")?;
        let function = module.getattr(name)?;

        let args = PyTuple::empty(py);
        for (idx, value) in values.iter().enumerate() {
            match value {
                Field::UInt(val) => args.set_item(idx, val)?,
                Field::Int(val) => args.set_item(idx, val)?,
                Field::Float(val) => args.set_item(idx, val.0)?,
                Field::Boolean(val) => args.set_item(idx, val)?,
                Field::String(val) => args.set_item(idx, val)?,
                _ => {
                    return Err(UnsupportedSqlError(GenericError(format!(
                        "Arg type {value} isn't supported"
                    ))));
                }
            }
        }
        let res = function.call1(args)?;
        Ok(match return_type {
            FieldType::UInt => Field::UInt(res.extract::<u64>()?),
            FieldType::Int => Field::Int(res.extract::<i64>()?),
            FieldType::Float => {
                let res = Field::Float(OrderedFloat::from(res.extract::<f64>()?));
                dbg!(&res);
                res
            }
            FieldType::Boolean => Field::Boolean(res.extract::<bool>()?),
            FieldType::String => Field::String(res.extract::<String>()?),
            FieldType::Text => Field::Text(res.extract::<String>()?),
            FieldType::Binary
            | FieldType::Decimal
            | FieldType::Timestamp
            | FieldType::Date
            | FieldType::Bson => {
                return Err(UnsupportedSqlError(GenericError(
                    "Unsupported return type for python udf".to_string(),
                )))
            }
        })
    })
}
