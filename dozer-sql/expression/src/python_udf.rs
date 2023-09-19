use crate::execution::Expression;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::pyo3::types::PyTuple;
use dozer_types::pyo3::Python;
use dozer_types::thiserror::{self, Error};
use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema};
use std::env;
use std::path::PathBuf;

const MODULE_NAME: &str = "python_udf";

#[derive(Debug, Error)]
pub enum Error {
    #[error(
        "Python UDF must have a return type. The syntax is: function_name<return_type>(arguments)"
    )]
    MissingReturnType,
    #[error("Missing 'VIRTUAL_ENV' environment var")]
    MissingVirtualEnv,
    #[error("PyO3 error: {0}")]
    PyO3(#[from] dozer_types::pyo3::PyErr),
    #[error("Unsupported return type: {0}")]
    UnsupportedReturnType(FieldType),
    #[error("Failed to parse return type: {0}")]
    FailedToParseReturnType(String),
}

pub fn evaluate_py_udf(
    schema: &Schema,
    name: &str,
    args: &[Expression],
    return_type: &FieldType,
    record: &Record,
) -> Result<Field, crate::error::Error> {
    let values = args
        .iter()
        .map(|arg| arg.evaluate(record, schema))
        .collect::<Result<Vec<_>, crate::error::Error>>()?;

    // Get the path of the Python interpreter in your virtual environment
    let env_path = env::var("VIRTUAL_ENV").map_err(|_| Error::MissingVirtualEnv)?;
    let py_path = format!("{env_path}/bin/python");
    // Set the `PYTHON_SYS_EXECUTABLE` environment variable
    env::set_var("PYTHON_SYS_EXECUTABLE", py_path);

    Python::with_gil(|py| -> Result<Field, Error> {
        // Get the directory containing the module
        let module_dir = PathBuf::from(env_path);
        // Import the `sys` module and append the module directory to the system path
        let sys = py.import("sys")?;
        let path = sys.getattr("path")?;
        path.call_method1("append", (module_dir.to_string_lossy(),))?;

        let module = py.import(MODULE_NAME)?;
        let function = module.getattr(name)?;

        let args = PyTuple::new(py, values);
        let res = function.call1(args)?;

        Ok(match return_type {
            FieldType::UInt => Field::UInt(res.extract::<u64>()?),
            FieldType::U128 => Field::U128(res.extract::<u128>()?),
            FieldType::Int => Field::Int(res.extract::<i64>()?),
            FieldType::I128 => Field::I128(res.extract::<i128>()?),
            FieldType::Float => Field::Float(OrderedFloat::from(res.extract::<f64>()?)),
            FieldType::Boolean => Field::Boolean(res.extract::<bool>()?),
            FieldType::String => Field::String(res.extract::<String>()?),
            FieldType::Text => Field::Text(res.extract::<String>()?),
            FieldType::Binary => Field::Binary(res.extract::<Vec<u8>>()?),
            FieldType::Decimal
            | FieldType::Date
            | FieldType::Timestamp
            | FieldType::Point
            | FieldType::Duration
            | FieldType::Json => return Err(Error::UnsupportedReturnType(*return_type)),
        })
    })
    .map_err(Into::into)
}
