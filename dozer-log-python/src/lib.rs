#![cfg(feature = "python-extension-module")]

use std::sync::Arc;

use dozer_log::{
    home_dir::HomeDir, reader::LogReader as DozerLogReader, schemas::load_schema,
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
    fn new(py: Python, home_dir: String, endpoint_name: String) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let home_dir = HomeDir::new(home_dir.as_ref(), Default::default());
            let migration_path = home_dir
                .find_latest_migration_path(&endpoint_name)
                .map_err(|(path, error)| {
                    PyException::new_err(format!("Failed to read {path:?}: {error}"))
                })?;
            let migration_path =
                migration_path.ok_or(PyException::new_err("No migration found"))?;

            let schema = load_schema(&migration_path.schema_path)
                .map_err(|e| PyException::new_err(e.to_string()))?;

            let log_path = migration_path.log_path;
            let name = log_path
                .parent()
                .and_then(|parent| parent.file_name().and_then(|file_name| file_name.to_str()))
                .unwrap_or("unknown")
                .to_string();
            let reader_result = DozerLogReader::new(&log_path, name, 0, None).await;
            let reader = reader_result.map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(LogReader {
                reader: Arc::new(Mutex::new(reader)),
                schema: schema.schema,
            })
        })
    }

    fn next_op<'py>(&mut self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let reader = self.reader.clone();
        let schema = self.schema.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let op = reader.lock().await.next_op().await.0;
            Python::with_gil(|py| mapper::map_executor_operation(op, &schema, py))
        })
    }
}

/// Python binding for reading Dozer logs
#[pymodule]
#[pyo3(crate = "dozer_types::pyo3")]
fn pydozer_log(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<LogReader>()?;
    Ok(())
}

mod mapper;
