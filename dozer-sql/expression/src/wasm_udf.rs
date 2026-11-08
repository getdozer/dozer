use crate::execution::Expression;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::thiserror::{self, Error as ThisError};
use dozer_types::types::{Field, FieldType, Record, Schema};
use std::path::PathBuf;
use std::sync::Arc;
use wasmtime::{Engine, Instance, Module, Store, Val};

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Wasmtime runtime error: {0}")]
    Wasmtime(#[from] wasmtime::Error),
    #[error("WASM file not found at path: {0}")]
    FileNotFound(PathBuf),
    #[error("Failed to read WASM file at {0}: {1}")]
    FileReadError(PathBuf, String),
    #[error("Exported WASM function '{0}' not found in module")]
    FunctionNotFound(String),
    #[error("Unsupported input field type: {0}")]
    UnsupportedInputType(String),
    #[error("Unsupported return field type: {0:?}")]
    UnsupportedReturnType(FieldType),
    #[error("WASM function execution returned no result")]
    NoResult,
    #[error("Failed to parse return type: {0}")]
    FailedToParseReturnType(String),
    #[error("WASM UDF must have a return type. Syntax: function_name<return_type>(arguments)")]
    MissingReturnType,
}

#[derive(Clone)]
pub struct WasmUdf {
    pub name: String,
    pub path: PathBuf,
    pub args: Vec<Expression>,
    pub return_type: FieldType,
    pub wasm_bytes: Arc<Vec<u8>>,
    engine: Engine,
    module: Module,
}

impl std::fmt::Debug for WasmUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmUdf")
            .field("name", &self.name)
            .field("path", &self.path)
            .field("args", &self.args)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl PartialEq for WasmUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.path == other.path
            && self.args == other.args
            && self.return_type == other.return_type
    }
}

impl WasmUdf {
    pub fn new(
        name: String,
        path: PathBuf,
        args: Vec<Expression>,
        return_type: FieldType,
    ) -> Result<Self, Error> {
        let wasm_bytes = if path.exists() {
            std::fs::read(&path)
                .map_err(|e| Error::FileReadError(path.clone(), e.to_string()))?
        } else {
            return Err(Error::FileNotFound(path));
        };

        let engine = Engine::default();
        let module = Module::new(&engine, &wasm_bytes)?;

        Ok(Self {
            name,
            path,
            args,
            return_type,
            wasm_bytes: Arc::new(wasm_bytes),
            engine,
            module,
        })
    }

    pub fn from_bytes(
        name: String,
        wasm_bytes: Vec<u8>,
        args: Vec<Expression>,
        return_type: FieldType,
    ) -> Result<Self, Error> {
        let engine = Engine::default();
        let module = Module::new(&engine, &wasm_bytes)?;

        Ok(Self {
            name,
            path: PathBuf::new(),
            args,
            return_type,
            wasm_bytes: Arc::new(wasm_bytes),
            engine,
            module,
        })
    }

    pub fn evaluate(
        &mut self,
        schema: &Schema,
        record: &Record,
    ) -> Result<Field, crate::error::Error> {
        let mut param_vals = Vec::with_capacity(self.args.len());
        for arg in self.args.iter_mut() {
            let field_val = arg.evaluate(record, schema)?;
            let val = field_to_val(&field_val).map_err(crate::error::Error::WasmUdf)?;
            param_vals.push(val);
        }

        let mut store = Store::new(&self.engine, ());
        let instance = Instance::new(&mut store, &self.module, &[])
            .map_err(Error::from)
            .map_err(crate::error::Error::WasmUdf)?;

        let func = instance
            .get_func(&mut store, &self.name)
            .ok_or_else(|| Error::FunctionNotFound(self.name.clone()))
            .map_err(crate::error::Error::WasmUdf)?;

        let results_len = func.ty(&store).results().len();
        let mut results = vec![Val::I64(0); results_len];

        func.call(&mut store, &param_vals, &mut results)
            .map_err(Error::from)
            .map_err(crate::error::Error::WasmUdf)?;

        if results.is_empty() {
            return Err(crate::error::Error::WasmUdf(Error::NoResult));
        }

        val_to_field(&results[0], &self.return_type).map_err(crate::error::Error::WasmUdf)
    }
}

pub fn field_to_val(field: &Field) -> Result<Val, Error> {
    match field {
        Field::Int(v) => Ok(Val::I64(*v)),
        Field::UInt(v) => Ok(Val::I64(*v as i64)),
        Field::Int8(v) => Ok(Val::I32(*v as i32)),
        Field::Float(v) => Ok(Val::F64(v.0.to_bits())),
        Field::Boolean(v) => Ok(Val::I32(if *v { 1 } else { 0 })),
        _ => Err(Error::UnsupportedInputType(format!("{:?}", field))),
    }
}

pub fn val_to_field(val: &Val, return_type: &FieldType) -> Result<Field, Error> {
    match return_type {
        FieldType::Int => {
            if let Some(i) = val.i64() {
                Ok(Field::Int(i))
            } else if let Some(i) = val.i32() {
                Ok(Field::Int(i as i64))
            } else {
                Err(Error::UnsupportedReturnType(*return_type))
            }
        }
        FieldType::Int8 => {
            if let Some(i) = val.i32() {
                Ok(Field::Int8(i as i8))
            } else if let Some(i) = val.i64() {
                Ok(Field::Int8(i as i8))
            } else {
                Err(Error::UnsupportedReturnType(*return_type))
            }
        }
        FieldType::UInt => {
            if let Some(i) = val.i64() {
                Ok(Field::UInt(i as u64))
            } else if let Some(i) = val.i32() {
                Ok(Field::UInt(i as u64))
            } else {
                Err(Error::UnsupportedReturnType(*return_type))
            }
        }
        FieldType::Float => {
            if let Some(f) = val.f64() {
                Ok(Field::Float(OrderedFloat(f)))
            } else if let Some(f) = val.f32() {
                Ok(Field::Float(OrderedFloat(f as f64)))
            } else {
                Err(Error::UnsupportedReturnType(*return_type))
            }
        }
        FieldType::Boolean => {
            if let Some(b) = val.i32() {
                Ok(Field::Boolean(b != 0))
            } else if let Some(b) = val.i64() {
                Ok(Field::Boolean(b != 0))
            } else {
                Err(Error::UnsupportedReturnType(*return_type))
            }
        }
        _ => Err(Error::UnsupportedReturnType(*return_type)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};

    #[test]
    fn test_wasm_udf_add() {
        // WAT for add(i64, i64) -> i64
        let wat_code = r#"
            (module
                (func (export "add") (param i64 i64) (result i64)
                    local.get 0
                    local.get 1
                    i64.add
                )
            )
        "#;
        let wasm_bytes = wat::parse_str(wat_code).expect("valid wat");

        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "a".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "b".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone();

        let record = Record::new(vec![Field::Int(20), Field::Int(22)]);

        let mut udf = WasmUdf::from_bytes(
            "add".to_string(),
            wasm_bytes,
            vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 1 },
            ],
            FieldType::Int,
        )
        .expect("should create wasm udf");

        let result = udf.evaluate(&schema, &record).expect("evaluation succeeds");
        assert_eq!(result, Field::Int(42));
    }

    #[test]
    fn test_wasm_udf_multiply_float() {
        // WAT for mul_float(f64, f64) -> f64
        let wat_code = r#"
            (module
                (func (export "mul_float") (param f64 f64) (result f64)
                    local.get 0
                    local.get 1
                    f64.mul
                )
            )
        "#;
        let wasm_bytes = wat::parse_str(wat_code).expect("valid wat");

        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "x".to_string(),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "y".to_string(),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone();

        let record = Record::new(vec![
            Field::Float(OrderedFloat(2.5)),
            Field::Float(OrderedFloat(4.0)),
        ]);

        let mut udf = WasmUdf::from_bytes(
            "mul_float".to_string(),
            wasm_bytes,
            vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 1 },
            ],
            FieldType::Float,
        )
        .expect("should create wasm udf");

        let result = udf.evaluate(&schema, &record).expect("evaluation succeeds");
        assert_eq!(result, Field::Float(OrderedFloat(10.0)));
    }
}
