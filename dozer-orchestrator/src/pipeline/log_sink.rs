use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
};

use dozer_core::{
    epoch::Epoch,
    errors::ExecutionError,
    executor::ExecutorOperation,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::{
    bytes::{BufMut, BytesMut},
    indicatif::MultiProgress,
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    types::{Operation, Schema, SchemaIdentifier},
};
use std::fs::OpenOptions;

#[derive(Debug, Clone)]
pub struct LogSinkSettings {
    pub pipeline_dir: PathBuf,
}

const SCHEMA_FILE_NAME: &str = "schemas.json";

#[derive(Debug, Clone)]
pub struct LogSinkFactory {
    settings: LogSinkSettings,
    api_endpoint: ApiEndpoint,
    multi_pb: MultiProgress,
}

impl LogSinkFactory {
    pub fn new(
        settings: LogSinkSettings,
        api_endpoint: ApiEndpoint,
        multi_pb: MultiProgress,
    ) -> Self {
        Self {
            settings,
            api_endpoint,
            multi_pb,
        }
    }

    fn get_output_schema(
        &self,
        mut input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        debug_assert!(input_schemas.len() == 1);
        let mut schema = input_schemas
            .remove(&DEFAULT_PORT_HANDLE)
            .expect("Input schema should be on default port");

        // Generated Cache index based on api_index
        let configured_index = create_primary_indexes(
            &schema,
            &self.api_endpoint.index.to_owned().unwrap_or_default(),
        )?;
        // Generated schema in SQL
        let upstream_index = schema.primary_index.clone();

        let index = match (configured_index.is_empty(), upstream_index.is_empty()) {
            (true, true) => vec![],
            (true, false) => upstream_index,
            (false, true) => configured_index,
            (false, false) => {
                if !upstream_index.eq(&configured_index) {
                    return Err(ExecutionError::MismatchPrimaryKey {
                        endpoint_name: self.api_endpoint.name.clone(),
                        expected: get_field_names(&schema, &upstream_index),
                        actual: get_field_names(&schema, &configured_index),
                    });
                }
                configured_index
            }
        };

        schema.primary_index = index;

        schema.identifier = Some(SchemaIdentifier {
            id: DEFAULT_PORT_HANDLE as u32,
            version: 1,
        });

        Ok(schema)
    }
}

impl SinkFactory<SchemaSQLContext> for LogSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(
        &self,
        input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        // Get output schema.
        let schema = self.get_output_schema(
            input_schemas
                .into_iter()
                .map(|(key, (schema, _))| (key, schema))
                .collect(),
        )?;

        schema.print().printstd();

        let path = Path::new(&self.settings.pipeline_dir).join(SCHEMA_FILE_NAME);
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        writeln!(file, "{}", serde_json::to_string(&schema).unwrap())
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        let log_path = get_endpoint_log_path(&self.settings.pipeline_dir, &self.api_endpoint.name);

        std::fs::create_dir_all(log_path.as_path())
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(Box::new(LogSink::new(
            Some(self.multi_pb.clone()),
            log_path,
        )?))
    }
}

#[derive(Debug)]
pub struct LogSink {
    multi_pb: Option<MultiProgress>,
    file: std::fs::File,
}

impl LogSink {
    pub fn new(multi_pb: Option<MultiProgress>, log_path: PathBuf) -> Result<Self, ExecutionError> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(log_path)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        Ok(Self { multi_pb, file })
    }
}

impl Sink for LogSink {
    fn process(&mut self, from_port: PortHandle, op: Operation) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::Op { op };
        let msg = dozer_types::bincode::serialize(&executor_operation)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        write_msg_to_file(&mut self.file, &msg)
    }

    fn commit(&mut self) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        };
        let msg = dozer_types::bincode::serialize(&executor_operation)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        write_msg_to_file(&mut self.file, &msg)
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::SnapshottingDone {};
        todo!("Write snapshotting done to log file.")
    }
}

fn create_primary_indexes(
    schema: &Schema,
    api_index: &ApiIndex,
) -> Result<Vec<usize>, ExecutionError> {
    let mut primary_index = Vec::new();
    for name in api_index.primary_key.iter() {
        let idx = schema
            .fields
            .iter()
            .position(|fd| fd.name == name.clone())
            .map_or(Err(ExecutionError::FieldNotFound(name.to_owned())), |p| {
                Ok(p)
            })?;

        primary_index.push(idx);
    }
    Ok(primary_index)
}

fn get_field_names(schema: &Schema, indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|idx| schema.fields[*idx].name.to_owned())
        .collect()
}

fn get_logs_path(pipeline_dir: &Path) -> PathBuf {
    pipeline_dir.join("logs")
}

fn get_endpoint_log_path(pipeline_dir: &Path, endpoint_name: &str) -> PathBuf {
    get_logs_path(pipeline_dir).join(endpoint_name.to_lowercase())
}

fn write_msg_to_file(file: &mut std::fs::File, msg: &[u8]) -> Result<(), ExecutionError> {
    let mut buf = BytesMut::with_capacity(msg.len() + 4);
    buf.put_u32_le(msg.len() as u32);
    buf.put_slice(msg);
    file.write_all(&buf)
        .map_err(|e| ExecutionError::InternalError(Box::new(e)))
}
