#![cfg(feature = "python-extension-module")]

use std::sync::Arc;

use dozer_log::{
    reader::{LogReader as DozerLogReader, LogReaderBuilder, LogReaderOptions},
    tokio::sync::Mutex,
};
use dozer_types::pyo3::{exceptions::PyException, prelude::*};

#[pyclass(crate = "dozer_types::pyo3")]
struct LogReader {
    reader: Arc<Mutex<DozerLogReader>>,
}

#[pymethods(crate = "dozer_types::pyo3")]
impl LogReader {
    #[allow(clippy::new_ret_no_self)]
    #[staticmethod]
    fn new(py: Python, server_addr: String, endpoint_name: String) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let reader_result =
                LogReaderBuilder::new(server_addr, LogReaderOptions::new(endpoint_name)).await;
            let reader = reader_result
                .map_err(|e| PyException::new_err(e.to_string()))?
                .build(0);
            Ok(LogReader {
                reader: Arc::new(Mutex::new(reader)),
            })
        })
    }

    fn next_op<'py>(&mut self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut reader = reader.lock().await;
            let schema = reader.schema.schema.clone();
            let op = reader.read_one().await;
            Python::with_gil(|py| {
                let op = op.map_err(|e| PyException::new_err(e.to_string()))?.op;
                mapper::map_executor_operation(op, &schema, py)
            })
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
