//! Sandboxed, fixed-arity numeric WebAssembly scalar functions.
use std::{fs::File, io::Read, path::PathBuf, sync::Arc};

use dozer_types::{
    models::udf_config::{UdfConfig, UdfType, WasmConfig},
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use wasmi::{
    core::{Trap, ValueType},
    Config, Engine, ExternType, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
    Value,
};

use crate::{
    error::Error,
    execution::{Expression, ExpressionType},
};

const MAX_MODULE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_FUEL: u64 = 10_000_000;
const MAX_MEMORY: usize = 64 * 1024 * 1024;

fn error(name: &str, message: impl std::fmt::Display) -> Error {
    Error::Wasm(format!("{name}: {message}"))
}

#[derive(Debug)]
struct Compiled {
    engine: Engine,
    module: Module,
    bytes: Vec<u8>,
    config: WasmConfig,
    export: String,
    params: Vec<ValueType>,
    result: ValueType,
}

impl Compiled {
    fn load(name: &str, config: &WasmConfig) -> Result<Self, Error> {
        if config.fuel == 0 || config.fuel > MAX_FUEL {
            return Err(error(
                name,
                format!("fuel must be between 1 and {MAX_FUEL}"),
            ));
        }
        if config.memory_limit == 0 || config.memory_limit > MAX_MEMORY {
            return Err(error(
                name,
                format!("memory_limit must be between 1 and {MAX_MEMORY} bytes"),
            ));
        }
        let mut path = PathBuf::from(&config.path);
        if path.extension().is_none() {
            path.set_extension("wasm");
        }
        let metadata = std::fs::metadata(&path)
            .map_err(|e| error(name, format!("{}: {e}", path.display())))?;
        if !metadata.is_file() || metadata.len() > MAX_MODULE_BYTES {
            return Err(error(
                name,
                "module must be a regular file of at most 10 MiB",
            ));
        }
        let file =
            File::open(&path).map_err(|e| error(name, format!("{}: {e}", path.display())))?;
        let mut bytes = Vec::new();
        file.take(MAX_MODULE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| error(name, e))?;
        if bytes.len() as u64 > MAX_MODULE_BYTES {
            return Err(error(name, "module exceeds 10 MiB"));
        }
        let mut engine_config = Config::default();
        engine_config.consume_fuel(true);
        let engine = Engine::new(&engine_config);
        let module = Module::new(&engine, &bytes[..]).map_err(|e| error(name, e))?;
        for import in module.imports() {
            let is_abort = import.module() == "env"
                && import.name() == "abort"
                && matches!(import.ty(), ExternType::Func(ty)
                    if ty.params() == [ValueType::I32; 4] && ty.results().is_empty());
            if !is_abort {
                return Err(error(
                    name,
                    format!("unsupported import {}.{}", import.module(), import.name()),
                ));
            }
        }
        let export = config.export.as_deref().unwrap_or(name).to_owned();
        let Some(ExternType::Func(ty)) = module.get_export(&export) else {
            return Err(error(name, format!("missing function export {export}")));
        };
        let numeric = |ty: &ValueType| {
            matches!(
                ty,
                ValueType::I32 | ValueType::I64 | ValueType::F32 | ValueType::F64
            )
        };
        if ty.results().len() != 1
            || !ty.params().iter().all(numeric)
            || !ty.results().iter().all(numeric)
        {
            return Err(error(
                name,
                "expected numeric i32/i64/f32/f64 parameters and exactly one numeric result",
            ));
        }
        let params = ty.params().to_vec();
        let result = ty.results()[0];
        let compiled = Self {
            engine,
            module,
            bytes,
            config: config.clone(),
            export,
            params,
            result,
        };
        // Validate allocation and start functions before any stream records are accepted.
        compiled.instantiate(name)?;
        Ok(compiled)
    }

    fn instantiate(&self, name: &str) -> Result<(Store<StoreLimits>, Instance), Error> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.config.memory_limit)
            .table_elements(4096)
            .memories(1)
            .tables(1)
            .instances(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        store
            .add_fuel(self.config.fuel)
            .map_err(|e| error(name, e))?;
        let mut linker = Linker::new(&self.engine);
        linker
            .func_wrap(
                "env",
                "abort",
                |_message: i32, _file: i32, _line: i32, _column: i32| {
                    Err::<(), Trap>(Trap::new("AssemblyScript abort"))
                },
            )
            .map_err(|e| error(name, e))?;
        let instance = linker
            .instantiate(&mut store, &self.module)
            .and_then(|instance| instance.start(&mut store))
            .map_err(|e| error(name, e))?;
        Ok((store, instance))
    }
}

/// Validate all configured WASM functions, including ones not referenced by SQL.
pub fn validate_udfs(udfs: &[UdfConfig]) -> Result<(), Error> {
    for udf in udfs {
        if let UdfType::Wasm(config) = &udf.config {
            if udfs
                .iter()
                .filter(|other| other.name.eq_ignore_ascii_case(&udf.name))
                .count()
                > 1
            {
                return Err(error(&udf.name, "duplicate SQL function name"));
            }
            let mut chars = udf.name.chars();
            if !chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(error(
                    &udf.name,
                    "SQL function name must be a simple identifier",
                ));
            }
            Compiled::load(&udf.name, config)?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct Udf {
    name: String,
    args: Vec<Expression>,
    compiled: Arc<Compiled>,
    nullable: bool,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.args == other.args
            && self.compiled.config == other.compiled.config
            && self.compiled.bytes == other.compiled.bytes
    }
}

impl Udf {
    pub fn new(
        name: String,
        config: &WasmConfig,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, Error> {
        let compiled = Arc::new(Compiled::load(&name, config)?);
        if args.len() != compiled.params.len() {
            return Err(error(
                &name,
                format!(
                    "expected {} arguments, got {}",
                    compiled.params.len(),
                    args.len()
                ),
            ));
        }
        let mut nullable = false;
        for (index, (arg, param)) in args.iter().zip(&compiled.params).enumerate() {
            if matches!(arg, Expression::Literal(Field::Null)) {
                nullable = true;
                continue;
            }
            let ty = arg.get_type(schema)?;
            // Some existing computed expressions under-report nullability.
            // Preserve exact metadata for fields/literals/our own UDFs, and do
            // not advertise a non-null result for a potentially nullable input.
            nullable |= ty.nullable
                || !matches!(
                    arg,
                    Expression::Column { .. } | Expression::Literal(_) | Expression::WasmUdf(_)
                );
            let valid = match param {
                ValueType::I32 | ValueType::I64 => matches!(
                    ty.return_type,
                    FieldType::Int
                        | FieldType::Int8
                        | FieldType::I128
                        | FieldType::UInt
                        | FieldType::U128
                        | FieldType::Boolean
                ),
                ValueType::F32 | ValueType::F64 => ty.return_type == FieldType::Float,
                _ => false,
            };
            if !valid {
                return Err(error(&name, format!("argument {index} has SQL type {}, expected {param:?}; use an explicit CAST", ty.return_type)));
            }
        }
        Ok(Self {
            name,
            args,
            compiled,
            nullable,
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        let ty = match self.compiled.result {
            ValueType::I32 | ValueType::I64 => FieldType::Int,
            _ => FieldType::Float,
        };
        ExpressionType::new(ty, self.nullable, SourceDefinition::Dynamic, false)
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!(
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|arg| arg.to_string(schema))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub fn evaluate(&mut self, record: &Record, schema: &Schema) -> Result<Field, Error> {
        let fields = self
            .args
            .iter_mut()
            .map(|arg| arg.evaluate(record, schema))
            .collect::<Result<Vec<_>, _>>()?;
        if fields.iter().any(|field| matches!(field, Field::Null)) {
            return Ok(Field::Null);
        }
        let params = fields
            .iter()
            .zip(&self.compiled.params)
            .enumerate()
            .map(|(index, (field, ty))| {
                to_value(field, *ty).ok_or_else(|| {
                    error(
                        &self.name,
                        format!("argument {index} is not a finite, in-range {ty:?}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Stores are never shared across rows. Re-evaluating a deleted/updated row
        // sees the same initialized globals and memory as its original insertion.
        let (mut store, instance) = self.compiled.instantiate(&self.name)?;
        let function = instance
            .get_func(&store, &self.compiled.export)
            .ok_or_else(|| error(&self.name, "function export disappeared"))?;
        let mut result = [Value::default(self.compiled.result)];
        function
            .call(&mut store, &params, &mut result)
            .map_err(|e| error(&self.name, e))?;
        match result[0] {
            Value::I32(value) => Ok(Field::Int(i64::from(value))),
            Value::I64(value) => Ok(Field::Int(value)),
            Value::F32(value) => finite_float(&self.name, f64::from(f32::from(value))),
            Value::F64(value) => finite_float(&self.name, f64::from(value)),
            _ => Err(error(&self.name, "unsupported result type")),
        }
    }
}

fn finite_float(name: &str, value: f64) -> Result<Field, Error> {
    if value.is_finite() {
        Ok(Field::Float(value.into()))
    } else {
        Err(error(name, "result is NaN or infinity"))
    }
}

fn to_value(field: &Field, ty: ValueType) -> Option<Value> {
    match ty {
        ValueType::I32 | ValueType::I64 => {
            let value = match field {
                Field::Int(v) => i128::from(*v),
                Field::Int8(v) => i128::from(*v),
                Field::I128(v) => *v,
                Field::UInt(v) => i128::from(*v),
                Field::U128(v) => i128::try_from(*v).ok()?,
                Field::Boolean(v) => i128::from(*v),
                _ => return None,
            };
            if ty == ValueType::I32 {
                Some(Value::I32(i32::try_from(value).ok()?))
            } else {
                Some(Value::I64(i64::try_from(value).ok()?))
            }
        }
        ValueType::F32 | ValueType::F64 => {
            let Field::Float(value) = field else {
                return None;
            };
            if !value.0.is_finite() {
                return None;
            }
            if ty == ValueType::F32 {
                let value = value.0 as f32;
                value.is_finite().then(|| Value::F32(value.into()))
            } else {
                Some(Value::F64(value.0.into()))
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
