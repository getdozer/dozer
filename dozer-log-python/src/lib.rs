#![cfg(feature = "python-extension-module")]

use std::sync::Arc;

use ::dozer_log::{
    get_endpoint_log_path, reader::LogReader as DozerLogReader, schemas::load_schema,
    tokio::sync::Mutex,
};
use dozer_types::{
    pyo3::{exceptions::PyException, prelude::*},
    types::Schema,
};

#[pyclass(crate = "dozer_types::pyo3")]
struct LogReader {
    reader: Arc<Mutex<DozerLogReader>>,
    schema: Schema,
}

#[pymethods(crate = "dozer_types::pyo3")]
impl LogReader {
    #[allow(clippy::new_ret_no_self)]
    #[staticmethod]
    fn new(py: Python, pipeline_dir: String, endpoint_name: String) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let schema = load_schema(pipeline_dir.as_ref(), &endpoint_name)
                .map_err(|e| PyException::new_err(e.to_string()))?;

            let log_path = get_endpoint_log_path(pipeline_dir.as_ref(), &endpoint_name);
            let name = log_path
                .parent()
                .and_then(|parent| parent.file_name().and_then(|file_name| file_name.to_str()))
                .unwrap_or("unknown");
            let reader_result = DozerLogReader::new(&log_path, name, 0, None).await;
            let reader = reader_result.map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(LogReader {
                reader: Arc::new(Mutex::new(reader)),
                schema,
            })
        })
    }

    fn next_op<'py>(&mut self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let reader = self.reader.clone();
        let schema = self.schema.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let op = reader.lock().await.next_op().await;
            Python::with_gil(|py| mapper::map_executor_operation(op, &schema, py))
        })
    }
}

/// Python binding for reading Dozer logs
#[pymodule]
#[pyo3(crate = "dozer_types::pyo3")]
fn dozer_log(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<LogReader>()?;
    Ok(())
}

mod mapper;
