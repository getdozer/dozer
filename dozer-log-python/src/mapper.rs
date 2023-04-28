use dozer_types::json_types::JsonValue;
use dozer_types::pyo3::types::PyList;

use dozer_types::pyo3::exceptions::PyTypeError;
use dozer_types::{
    epoch::ExecutorOperation,
    pyo3::{types::PyDict, Py, PyAny, PyResult, Python, ToPyObject},
    types::{DozerPoint, Field, Operation, Record, Schema},
};

pub fn map_executor_operation(
    op: ExecutorOperation,
    schema: &Schema,
    py: Python,
) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);

    match op {
        ExecutorOperation::Op { op } => {
            result.set_item("type", "op")?;
            result.set_item("op", map_op(op, schema, py)?)?;
        }
        ExecutorOperation::Commit { .. } => {
            result.set_item("type", "commit")?;
        }
        ExecutorOperation::SnapshottingDone {} => {
            result.set_item("type", "snapshotting_done")?;
        }
        ExecutorOperation::Terminate => {
            result.set_item("type", "terminate")?;
        }
    }

    Ok(result.into())
}

fn map_op<'py>(op: Operation, schema: &Schema, py: Python<'py>) -> PyResult<&'py PyDict> {
    let result = PyDict::new(py);

    match op {
        Operation::Insert { new } => {
            result.set_item("type", "insert")?;
            result.set_item("new", map_record(new, schema, py)?)?;
        }
        Operation::Delete { old } => {
            result.set_item("type", "delete")?;
            result.set_item("old", map_record(old, schema, py)?)?;
        }
        Operation::Update { old, new } => {
            result.set_item("type", "update")?;
            result.set_item("old", map_record(old, schema, py)?)?;
            result.set_item("new", map_record(new, schema, py)?)?;
        }
    }

    Ok(result)
}

fn map_record<'py>(record: Record, schema: &Schema, py: Python<'py>) -> PyResult<&'py PyDict> {
    let result = PyDict::new(py);

    for (field, value) in schema.fields.iter().zip(record.values) {
        result.set_item(&field.name, map_value(value, py)?)?;
    }

    Ok(result)
}

fn map_value(value: Field, py: Python) -> PyResult<Py<PyAny>> {
    match value {
        Field::UInt(v) => Ok(v.to_object(py)),
        Field::U128(v) => Ok(v.to_object(py)),
        Field::Int(v) => Ok(v.to_object(py)),
        Field::I128(v) => Ok(v.to_object(py)),
        Field::Float(v) => Ok(v.to_object(py)),
        Field::Boolean(v) => Ok(v.to_object(py)),
        Field::String(v) => Ok(v.to_object(py)),
        Field::Text(v) => Ok(v.to_object(py)),
        Field::Binary(v) => Ok(v.to_object(py)),
        Field::Decimal(v) => Ok(v.to_string().to_object(py)),
        Field::Timestamp(v) => Ok(v.to_string().to_object(py)),
        Field::Date(v) => Ok(v.to_string().to_object(py)),
        Field::Json(v) => map_json_py(v, py),
        Field::Point(v) => map_point(v, py),
        Field::Duration(v) => Ok(v.to_string().to_object(py)),
        Field::Null => Ok(py.None()),
    }
}

fn map_json_py(val: JsonValue, py: Python) -> PyResult<Py<PyAny>> {
    match val {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.to_object(py)),
        JsonValue::Number(n) => Ok(n.to_object(py)),
        JsonValue::String(s) => Ok(s.to_object(py)),
        JsonValue::Array(a) => Ok(PyList::new(
            py,
            a.into_iter().map(|val| map_json_py(val, py).unwrap()),
        )
        .to_object(py)),
        JsonValue::Object(o) => {
            let obj = PyDict::new(py);
            for (key, val) in o {
                obj.set_item(key, map_json_py(val, py).unwrap()).map_or(
                    Err(PyTypeError::new_err("Json Object type conversion error").value(py)),
                    Ok,
                )?;
            }
            Ok(obj.to_object(py))
        }
    }
}

fn map_point(point: DozerPoint, py: Python) -> PyResult<Py<PyAny>> {
    let result = PyDict::new(py);
    result.set_item("x", point.0.x().0)?;
    result.set_item("y", point.0.y().0)?;
    Ok(result.into())
}
