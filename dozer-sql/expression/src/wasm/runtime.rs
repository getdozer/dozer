use std::{fs, path::Path, sync::Arc};

use dozer_types::{
    ordered_float::OrderedFloat,
    thiserror,
    types::{Field, FieldType},
};
use wasmi::{ core::ValueType, Caller, Engine, Func, Instance, Linker, Module, Store, Val };

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read WASM module from '{path}': {source}")]
    ReadModule {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to compile or instantiate WASM module '{module}': {source}")]
    WasmEngine {
        module: String,
        #[source]
        source: wasmi::Error,
    },
    #[error("WASM module '{module}' does not export function '{function_name}'")]
    MissingExport {
        module: String,
        function_name: String,
    },
    #[error("WASM function '{function_name}' must return exactly 1 scalar value, got {actual}")]
    InvalidReturnArity {
        function_name: String,
        actual: usize,
    },
    #[error("unsupported WASM value type '{0:?}' in function '{1}'")]
    UnsupportedValueType(ValueType, String),
}

#[derive(Debug, Clone, Copy)]
pub struct WasmDescriptor<'a> {
    pub module_path: &'a str,
    pub function_name: &'a str,
}

#[derive(Debug, Clone)]
pub struct CompiledWasmModule {
    pub module_path: String,
    pub engine: Engine,
    pub module: Arc<Module>,
    pub param_types: Vec<ValueType>,
    pub return_type: FieldType,
    pub wasm_return_type: ValueType,
}

pub fn load_compiled_module(desc: WasmDescriptor<'_>) -> Result<CompiledWasmModule, Error> {
    let bytes = read_wasm_bytes(desc.module_path)?;
    compile_wasm_bytes(desc, &bytes)
}

pub fn compile_wasm_bytes(
    desc: WasmDescriptor<'_>,
    bytes: &[u8],
) -> Result<CompiledWasmModule, Error> {
    let engine = Engine::default();
    let module = Module::new(&engine, bytes).map_err(|source| Error::WasmEngine {
        module: desc.module_path.to_string(),
        source,
    })?;
    let (_, func) = instantiate_function(&engine, &module, desc)?;
    let store = Store::new(&engine, ());
    let func_type = func.ty(&store);
    if func_type.results().len() != 1 {
        return Err(Error::InvalidReturnArity {
            function_name: desc.function_name.to_string(),
            actual: func_type.results().len(),
        });
    }
    let wasm_return_type = func_type.results()[0];
    let return_type = map_wasm_return_type(wasm_return_type, desc.function_name)?;
    Ok(CompiledWasmModule {
        module_path: desc.module_path.to_string(),
        engine,
        module: Arc::new(module),
        param_types: func_type.params().to_vec(),
        return_type,
        wasm_return_type,
    })
}

fn read_wasm_bytes(module_path: &str) -> Result<Vec<u8>, Error> {
    let direct = Path::new(module_path);
    if direct.exists() {
        return fs::read(direct).map_err(|source| Error::ReadModule {
            path: module_path.to_string(),
            source,
        });
    }
    let with_ext = format!("{module_path}.wasm");
    let fallback = Path::new(&with_ext);
    if fallback.exists() {
        return fs::read(fallback).map_err(|source| Error::ReadModule {
            path: with_ext,
            source,
        });
    }
    fs::read(direct).map_err(|source| Error::ReadModule {
        path: module_path.to_string(),
        source,
    })
}

pub fn instantiate_function(
    engine: &Engine,
    module: &Module,
    desc: WasmDescriptor<'_>,
) -> Result<(Store<()>, Func), Error> {
    let mut store = Store::new(engine, ());
    let mut linker = <Linker<()>>::new(engine);
    // Register AssemblyScript's standard runtime abort hook so compiled AssemblyScript modules instantiate cleanly.
    let _ = linker.func_wrap(
        "env",
        "abort",
        |_caller: Caller<'_, ()>, _msg: i32, _file: i32, _line: i32, _col: i32| {},
    );
    let instance: Instance = linker
        .instantiate(&mut store, module)
        .and_then(|pre| pre.start(&mut store))
        .map_err(|source| Error::WasmEngine {
            module: desc.module_path.to_string(),
            source,
        })?;
    let func = instance
        .get_func(&store, desc.function_name)
        .ok_or_else(|| Error::MissingExport {
            module: desc.module_path.to_string(),
            function_name: desc.function_name.to_string(),
        })?;
    Ok((store, func))
}

fn map_wasm_return_type(typ: ValueType, function_name: &str) -> Result<FieldType, Error> {
    match typ {
        ValueType::I32 | ValueType::I64 => Ok(FieldType::Int),
        ValueType::F32 | ValueType::F64 => Ok(FieldType::Float),
        other => Err(Error::UnsupportedValueType(other, function_name.to_string())),
    }
}

pub fn field_to_wasm_val(
    field: Field,
    target: ValueType,
    function_name: &str,
    argument_index: usize,
) -> Result<Val, crate::error::Error> {
    match (target, field) {
        (ValueType::I32, Field::Int(v)) => i32::try_from(v).map(Val::I32).map_err(|_| {
            crate::error::Error::InvalidFunctionArgument {
                function_name: function_name.to_string(),
                argument_index,
                argument: Field::Int(v),
            }
        }),
        (ValueType::I32, Field::UInt(v)) => i32::try_from(v).map(Val::I32).map_err(|_| {
            crate::error::Error::InvalidFunctionArgument {
                function_name: function_name.to_string(),
                argument_index,
                argument: Field::UInt(v),
            }
        }),
        (ValueType::I32, Field::Boolean(b)) => Ok(Val::I32(i32::from(b))),
        (ValueType::I64, Field::Int(v)) => Ok(Val::I64(v)),
        (ValueType::I64, Field::UInt(v)) => i64::try_from(v).map(Val::I64).map_err(|_| {
            crate::error::Error::InvalidFunctionArgument {
                function_name: function_name.to_string(),
                argument_index,
                argument: Field::UInt(v),
            }
        }),
        (ValueType::I64, Field::Boolean(b)) => Ok(Val::I64(i64::from(b))),
        (ValueType::F32, Field::Float(v)) => Ok(Val::F32((v.0 as f32).into())),
        (ValueType::F32, Field::Int(v)) => Ok(Val::F32((v as f32).into())),
        (ValueType::F64, Field::Float(v)) => Ok(Val::F64(v.0.into())),
        (ValueType::F64, Field::Int(v)) => Ok(Val::F64((v as f64).into())),
        (_, invalid) => Err(crate::error::Error::InvalidFunctionArgument {
            function_name: function_name.to_string(),
            argument_index,
            argument: invalid,
        }),
    }
}

pub fn wasm_val_to_field(val: &Val, function_name: &str) -> Result<Field, Error> {
    match val {
        Val::I32(v) => Ok(Field::Int(i64::from(*v))),
        Val::I64(v) => Ok(Field::Int(*v)),
        Val::F32(v) => Ok(Field::Float(OrderedFloat(f64::from(f32::from(*v))))),
        Val::F64(v) => Ok(Field::Float(OrderedFloat(f64::from(*v)))),
        other => Err(Error::UnsupportedValueType(
            other.ty(),
            function_name.to_string(),
        )),
    }
}
