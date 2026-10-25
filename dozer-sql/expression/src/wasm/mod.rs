//! Numeric, deterministic-per-call WASM scalar functions. No WASI is linked.

use std::{fs::File, io::Read, path::PathBuf, sync::Arc};

use dozer_types::{
    models::udf_config::WasmConfig,
    ordered_float::OrderedFloat,
    thiserror::{self, Error},
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use wasmi::{
    core::ValType, Config, EnforcedLimits, Engine, ExternType, FuncType, Instance, Linker, Module,
    StackLimits, Store, StoreLimits, StoreLimitsBuilder, Val,
};

use crate::execution::{Expression, ExpressionType};

const MAX_MODULE_BYTES: usize = 4 * 1024 * 1024;
const MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
const MAX_TABLE_ELEMENTS: u32 = 4096;
const CALL_FUEL: u64 = 1_000_000;

#[derive(Debug, Error)]
pub enum Error {
    #[error("WASM module {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Invalid WASM configuration: {0}")]
    Config(String),
    #[error("Invalid WASM module: {0}")]
    Module(#[source] wasmi::Error),
    #[error("WASM export {0} must be a function")]
    MissingExport(String),
    #[error("Unsupported WASM signature for {0}: expected numeric parameters and exactly one numeric result")]
    Signature(String),
    #[error("WASM import is not allowed: {0}::{1}")]
    Import(String, String),
    #[error("WASM {function} argument {index}: expected {expected:?}; {reason}")]
    Argument {
        function: String,
        index: usize,
        expected: ValType,
        reason: String,
    },
    #[error("WASM {function} execution failed: {source}")]
    Runtime {
        function: String,
        source: wasmi::Error,
    },
    #[error("WASM {0} returned a nonfinite float")]
    NonfiniteResult(String),
}

#[derive(Debug)]
pub(crate) struct Compiled {
    name: String,
    bytes: Vec<u8>,
    engine: Engine,
    module: Module,
    signature: FuncType,
}

#[derive(Debug, Clone)]
pub struct Udf {
    compiled: Arc<Compiled>,
    args: Vec<Expression>,
    nullable: bool,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args
            && self.compiled.name == other.compiled.name
            && (Arc::ptr_eq(&self.compiled, &other.compiled)
                || self.compiled.bytes == other.compiled.bytes)
    }
}

impl Compiled {
    pub(crate) fn load(name: String, config: &WasmConfig) -> Result<Arc<Self>, Error> {
        if name.is_empty()
            || !name
                .bytes()
                .enumerate()
                .all(|(i, b)| b == b'_' || b.is_ascii_lowercase() || (i > 0 && b.is_ascii_digit()))
        {
            return Err(Error::Config(
                "function names must be lowercase ASCII SQL identifiers".into(),
            ));
        }
        if config.path.is_empty() {
            return Err(Error::Config("module path is empty".into()));
        }
        if crate::builder::ExpressionBuilder::is_builtin_function(&name) {
            return Err(Error::Config(format!(
                "{name} conflicts with a built-in SQL function"
            )));
        }
        let mut path = PathBuf::from(&config.path);
        if path.extension().is_none() {
            path.set_extension("wasm");
        }
        let read_error = |source| Error::Read {
            path: path.clone(),
            source,
        };
        let metadata = std::fs::metadata(&path).map_err(read_error)?;
        if !metadata.is_file() || metadata.len() > MAX_MODULE_BYTES as u64 {
            return Err(Error::Config(format!(
                "{} must be a regular file of at most {MAX_MODULE_BYTES} bytes",
                path.display()
            )));
        }
        // Limit the actual read too: the file may grow after metadata was checked.
        let mut bytes = Vec::new();
        File::open(&path)
            .map_err(read_error)?
            .take((MAX_MODULE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(read_error)?;
        if bytes.len() > MAX_MODULE_BYTES {
            return Err(Error::Config("module exceeds byte limit".into()));
        }

        let mut settings = Config::default();
        settings
            .consume_fuel(true)
            .enforced_limits(EnforcedLimits::strict());
        settings.set_stack_limits(StackLimits {
            initial_value_stack_height: 128,
            maximum_value_stack_height: 16 * 1024,
            maximum_recursion_depth: 128,
        });
        let engine = Engine::new(&settings);
        let module = Module::new(&engine, &bytes).map_err(Error::Module)?;
        for import in module.imports() {
            let abort = matches!(import.ty(), ExternType::Func(ty)
                if ty.params() == [ValType::I32; 4] && ty.results().is_empty());
            if import.module() != "env" || import.name() != "abort" || !abort {
                return Err(Error::Import(import.module().into(), import.name().into()));
            }
        }
        let signature = match module.get_export(&name) {
            Some(ExternType::Func(ty)) => ty,
            _ => return Err(Error::MissingExport(name)),
        };
        let numeric = |ty: &ValType| {
            matches!(
                ty,
                ValType::I32 | ValType::I64 | ValType::F32 | ValType::F64
            )
        };
        if signature.results().len() != 1
            || !signature.params().iter().all(numeric)
            || !signature.results().iter().all(numeric)
        {
            return Err(Error::Signature(name));
        }
        let compiled = Arc::new(Self {
            name,
            bytes,
            engine,
            module,
            signature,
        });
        // Validate initialization, limits and start functions before any sources run.
        compiled.instantiate()?;
        Ok(compiled)
    }

    fn runtime_error(&self, source: wasmi::Error) -> Error {
        Error::Runtime {
            function: self.name.clone(),
            source,
        }
    }

    fn instantiate(&self) -> Result<(Store<StoreLimits>, Instance), Error> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_MEMORY_BYTES)
            .memories(1)
            .table_elements(MAX_TABLE_ELEMENTS)
            .tables(1)
            .instances(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        store
            .set_fuel(CALL_FUEL)
            .map_err(|e| self.runtime_error(e))?;
        let mut linker = Linker::<StoreLimits>::new(&self.engine);
        // AssemblyScript's only allowed host import. Never dereference its pointers.
        linker
            .func_wrap(
                "env",
                "abort",
                |_: i32, _: i32, _: i32, _: i32| -> Result<(), wasmi::Error> {
                    Err(wasmi::Error::new("AssemblyScript abort"))
                },
            )
            .map_err(|e| self.runtime_error(e.into()))?;
        let instance = linker
            .instantiate(&mut store, &self.module)
            .and_then(|pre| pre.start(&mut store))
            .map_err(|e| self.runtime_error(e))?;
        Ok((store, instance))
    }
}

impl Udf {
    pub fn new(
        name: String,
        config: &WasmConfig,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, crate::error::Error> {
        Self::bind(Compiled::load(name, config)?, args, schema)
    }

    pub(crate) fn bind(
        compiled: Arc<Compiled>,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, crate::error::Error> {
        if args.len() != compiled.signature.params().len() {
            return Err(crate::error::Error::InvalidNumberOfArguments {
                function_name: compiled.name.clone(),
                expected: compiled.signature.params().len()..compiled.signature.params().len() + 1,
                actual: args.len(),
            });
        }
        let mut nullable = false;
        for (index, (arg, expected)) in args.iter().zip(compiled.signature.params()).enumerate() {
            if matches!(arg, Expression::Literal(Field::Null)) {
                nullable = true;
                continue;
            }
            let ty = arg.get_type(schema)?;
            nullable |= ty.nullable;
            let supported = match expected {
                ValType::I32 | ValType::I64 => {
                    (*expected == ValType::I32 && ty.return_type == FieldType::Boolean)
                        || matches!(
                            ty.return_type,
                            FieldType::Int
                                | FieldType::Int8
                                | FieldType::I128
                                | FieldType::UInt
                                | FieldType::U128
                        )
                }
                ValType::F32 | ValType::F64 => ty.return_type == FieldType::Float,
                _ => false,
            };
            if !supported {
                return Err(Error::Argument {
                    function: compiled.name.clone(),
                    index,
                    expected: *expected,
                    reason: format!("incompatible SQL type {}", ty.return_type),
                }
                .into());
            }
            if let Some(warning) = conversion_warning(ty.return_type, *expected) {
                dozer_types::log::warn!("WASM {} argument {index}: {warning}", compiled.name);
            }
        }
        Ok(Self {
            compiled,
            args,
            nullable,
        })
    }

    pub fn evaluate(
        &mut self,
        record: &Record,
        schema: &Schema,
    ) -> Result<Field, crate::error::Error> {
        let fields = self
            .args
            .iter_mut()
            .map(|arg| arg.evaluate(record, schema))
            .collect::<Result<Vec<_>, _>>()?;
        if fields.contains(&Field::Null) {
            return Ok(Field::Null);
        }
        let params = fields
            .iter()
            .zip(self.compiled.signature.params())
            .enumerate()
            .map(|(index, (field, expected))| self.convert_argument(field, *expected, index))
            .collect::<Result<Vec<_>, _>>()?;
        // A fresh store resets globals, memory and fuel, including after a prior trap.
        let (mut store, instance) = self.compiled.instantiate()?;
        let function = instance
            .get_func(&store, &self.compiled.name)
            .ok_or_else(|| Error::MissingExport(self.compiled.name.clone()))?;
        let mut results = [Val::default(self.compiled.signature.results()[0])];
        function
            .call(&mut store, &params, &mut results)
            .map_err(|e| self.compiled.runtime_error(e))?;
        match results[0] {
            Val::I32(value) => Ok(Field::Int(i64::from(value))),
            Val::I64(value) => Ok(Field::Int(value)),
            Val::F32(value) => self
                .float_result(f64::from(f32::from(value)))
                .map_err(Into::into),
            Val::F64(value) => self.float_result(f64::from(value)).map_err(Into::into),
            _ => Err(Error::Signature(self.compiled.name.clone()).into()),
        }
    }

    fn float_result(&self, value: f64) -> Result<Field, Error> {
        if !value.is_finite() {
            return Err(Error::NonfiniteResult(self.compiled.name.clone()));
        }
        Ok(Field::Float(OrderedFloat(value)))
    }

    fn convert_argument(
        &self,
        field: &Field,
        expected: ValType,
        index: usize,
    ) -> Result<Val, Error> {
        let bad_argument = || Error::Argument {
            function: self.compiled.name.clone(),
            index,
            expected,
            reason: "incompatible value, nonfinite float, or value outside the ABI range".into(),
        };
        match expected {
            ValType::I32 | ValType::I64 => {
                let integer = match field {
                    Field::Boolean(value) if expected == ValType::I32 => {
                        i128::from(u8::from(*value))
                    }
                    Field::Int(value) => i128::from(*value),
                    Field::Int8(value) => i128::from(*value),
                    Field::UInt(value) => i128::from(*value),
                    Field::I128(value) => *value,
                    Field::U128(value) => i128::try_from(*value).map_err(|_| bad_argument())?,
                    _ => return Err(bad_argument()),
                };
                match expected {
                    ValType::I32 => i32::try_from(integer)
                        .map(Val::I32)
                        .map_err(|_| bad_argument()),
                    _ => i64::try_from(integer)
                        .map(Val::I64)
                        .map_err(|_| bad_argument()),
                }
            }
            ValType::F32 | ValType::F64 => {
                let value = match field {
                    Field::Float(value) if value.0.is_finite() => value.0,
                    _ => return Err(bad_argument()),
                };
                if expected == ValType::F32 {
                    let narrow = value as f32;
                    if !narrow.is_finite() {
                        return Err(bad_argument());
                    }
                    Ok(Val::F32(narrow.into()))
                } else {
                    Ok(Val::F64(value.into()))
                }
            }
            _ => Err(bad_argument()),
        }
    }

    pub fn get_type(&self) -> ExpressionType {
        let return_type = match self.compiled.signature.results()[0] {
            ValType::I32 | ValType::I64 => FieldType::Int,
            _ => FieldType::Float,
        };
        ExpressionType::new(return_type, self.nullable, SourceDefinition::Dynamic, false)
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!(
            "{}({})",
            self.compiled.name,
            self.args
                .iter()
                .map(|arg| arg.to_string(schema))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

fn conversion_warning(input: FieldType, output: ValType) -> Option<&'static str> {
    match (input, output) {
        (FieldType::Int | FieldType::I128 | FieldType::UInt | FieldType::U128, ValType::I32)
        | (FieldType::I128 | FieldType::UInt | FieldType::U128, ValType::I64) => Some(
            "integer conversion narrows the accepted range; out-of-range values fail at execution",
        ),
        (FieldType::Float, ValType::F32) => Some(
            "conversion to f32 may lose precision; overflow and nonfinite values fail at execution",
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
