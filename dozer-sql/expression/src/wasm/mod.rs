use dozer_types::thiserror;
use dozer_types::types::{Field, FieldType, Record, Schema};
use std::fmt;
use wasmtime::{Engine, Instance, Module, Store, Val, ValType};

use crate::execution::{Expression, ExpressionType};

#[derive(Clone)]
pub struct WasmSession {
    module_path: String,
    function_name: String,
    engine: Engine,
    module: Module,
    params: Vec<ValType>,
}

impl fmt::Debug for WasmSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmSession")
            .field("module_path", &self.module_path)
            .field("function_name", &self.function_name)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl PartialEq for WasmSession {
    fn eq(&self, other: &Self) -> bool {
        self.module_path == other.module_path
            && self.function_name == other.function_name
            && self.params == other.params
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to load WASM module {path}: {source}")]
    LoadModule {
        path: String,
        #[source]
        source: wasmtime::Error,
    },
    #[error("failed to instantiate WASM module {path}: {source}")]
    Instantiate {
        path: String,
        #[source]
        source: wasmtime::Error,
    },
    #[error("WASM function {function} is missing from module {path}")]
    MissingFunction { path: String, function: String },
    #[error("WASM function {function} must return exactly one value, got {actual}")]
    InvalidResultCount { function: String, actual: usize },
    #[error("WASM function {function} expected {expected} arguments, got {actual}")]
    InvalidArgumentCount {
        function: String,
        expected: usize,
        actual: usize,
    },
    #[error("WASM function {function} argument {index} cannot accept Dozer type {field_type:?} as {wasm_type:?}")]
    UnsupportedArgumentType {
        function: String,
        index: usize,
        field_type: FieldType,
        wasm_type: ValType,
    },
    #[error("WASM function {function} result type {wasm_type:?} is not supported")]
    UnsupportedResultType {
        function: String,
        wasm_type: ValType,
    },
    #[error("WASM argument {index} value {field:?} cannot be represented as {wasm_type:?}")]
    ArgumentOverflow {
        index: usize,
        field: Field,
        wasm_type: ValType,
    },
    #[error("WASM argument {index} value {field:?} cannot be passed as {wasm_type:?}")]
    InvalidArgumentValue {
        index: usize,
        field: Field,
        wasm_type: ValType,
    },
    #[error("WASM function {function} trapped: {source}")]
    Trap {
        function: String,
        #[source]
        source: wasmtime::Error,
    },
}

pub fn prepare_wasm_session(
    module_path: &str,
    function_name: &str,
    args: &[Expression],
    schema: &Schema,
) -> Result<(WasmSession, FieldType), crate::error::Error> {
    let engine = Engine::default();
    let module = Module::from_file(&engine, module_path).map_err(|source| Error::LoadModule {
        path: module_path.to_string(),
        source,
    })?;
    let mut store = Store::new(&engine, ());
    let instance =
        Instance::new(&mut store, &module, &[]).map_err(|source| Error::Instantiate {
            path: module_path.to_string(),
            source,
        })?;
    let function = instance
        .get_func(&mut store, function_name)
        .ok_or_else(|| Error::MissingFunction {
            path: module_path.to_string(),
            function: function_name.to_string(),
        })?;
    let function_type = function.ty(&store);
    let params: Vec<_> = function_type.params().collect();
    let results: Vec<_> = function_type.results().collect();
    if results.len() != 1 {
        return Err(Error::InvalidResultCount {
            function: function_name.to_string(),
            actual: results.len(),
        }
        .into());
    }
    if params.len() != args.len() {
        return Err(Error::InvalidArgumentCount {
            function: function_name.to_string(),
            expected: params.len(),
            actual: args.len(),
        }
        .into());
    }

    for (index, (arg, wasm_type)) in args.iter().zip(params.iter()).enumerate() {
        let ExpressionType { return_type, .. } = arg.get_type(schema)?;
        if !can_pass_as_wasm(return_type, wasm_type.clone()) {
            return Err(Error::UnsupportedArgumentType {
                function: function_name.to_string(),
                index,
                field_type: return_type,
                wasm_type: wasm_type.clone(),
            })?;
        }
    }

    let result_type = results[0].clone();
    let return_type =
        field_type_from_wasm_result(function_name, result_type.clone()).ok_or_else(|| {
            Error::UnsupportedResultType {
                function: function_name.to_string(),
                wasm_type: result_type,
            }
        })?;

    Ok((
        WasmSession {
            module_path: module_path.to_string(),
            function_name: function_name.to_string(),
            engine,
            module,
            params,
        },
        return_type,
    ))
}

pub fn evaluate_wasm_udf(
    schema: &Schema,
    args: &mut [Expression],
    record: &Record,
    session: &WasmSession,
) -> Result<Field, crate::error::Error> {
    let mut values = Vec::with_capacity(args.len());
    for (index, (arg, wasm_type)) in args.iter_mut().zip(session.params.iter()).enumerate() {
        let field = arg.evaluate(record, schema)?;
        values.push(field_to_wasm_val(index, field, wasm_type.clone())?);
    }

    let mut store = Store::new(&session.engine, ());
    let instance =
        Instance::new(&mut store, &session.module, &[]).map_err(|source| Error::Instantiate {
            path: session.module_path.clone(),
            source,
        })?;
    let function = instance
        .get_func(&mut store, &session.function_name)
        .ok_or_else(|| Error::MissingFunction {
            path: session.module_path.clone(),
            function: session.function_name.clone(),
        })?;
    let mut results = [Val::I32(0)];
    function
        .call(&mut store, &values, &mut results)
        .map_err(|source| Error::Trap {
            function: session.function_name.clone(),
            source,
        })?;
    Ok(wasm_val_to_field(results[0].clone()))
}

fn can_pass_as_wasm(field_type: FieldType, wasm_type: ValType) -> bool {
    matches!(
        (field_type, wasm_type),
        (
            FieldType::Int | FieldType::Int8 | FieldType::UInt | FieldType::I128 | FieldType::U128,
            ValType::I32 | ValType::I64
        ) | (FieldType::Boolean, ValType::I32 | ValType::I64)
            | (FieldType::Float, ValType::F32 | ValType::F64)
    )
}

fn field_type_from_wasm_result(function_name: &str, wasm_type: ValType) -> Option<FieldType> {
    let _ = function_name;
    match wasm_type {
        ValType::I32 | ValType::I64 => Some(FieldType::Int),
        ValType::F32 | ValType::F64 => Some(FieldType::Float),
        _ => None,
    }
}

fn field_to_wasm_val(index: usize, field: Field, wasm_type: ValType) -> Result<Val, Error> {
    match wasm_type {
        ValType::I32 => integer_to_i32(index, field).map(Val::I32),
        ValType::I64 => integer_to_i64(index, field).map(Val::I64),
        ValType::F32 => match field {
            Field::Float(value) => Ok(Val::F32((value.0 as f32).to_bits())),
            field => Err(Error::InvalidArgumentValue {
                index,
                field,
                wasm_type,
            }),
        },
        ValType::F64 => match field {
            Field::Float(value) => Ok(Val::F64(value.0.to_bits())),
            field => Err(Error::InvalidArgumentValue {
                index,
                field,
                wasm_type,
            }),
        },
        _ => Err(Error::InvalidArgumentValue {
            index,
            field,
            wasm_type,
        }),
    }
}

fn integer_to_i32(index: usize, field: Field) -> Result<i32, Error> {
    match field {
        Field::Int(value) => {
            i32::try_from(value).map_err(|_| overflow(index, Field::Int(value), ValType::I32))
        }
        Field::Int8(value) => Ok(i32::from(value)),
        Field::UInt(value) => {
            i32::try_from(value).map_err(|_| overflow(index, Field::UInt(value), ValType::I32))
        }
        Field::I128(value) => {
            i32::try_from(value).map_err(|_| overflow(index, Field::I128(value), ValType::I32))
        }
        Field::U128(value) => {
            i32::try_from(value).map_err(|_| overflow(index, Field::U128(value), ValType::I32))
        }
        Field::Boolean(value) => Ok(if value { 1 } else { 0 }),
        field => Err(Error::InvalidArgumentValue {
            index,
            field,
            wasm_type: ValType::I32,
        }),
    }
}

fn integer_to_i64(index: usize, field: Field) -> Result<i64, Error> {
    match field {
        Field::Int(value) => Ok(value),
        Field::Int8(value) => Ok(i64::from(value)),
        Field::UInt(value) => {
            i64::try_from(value).map_err(|_| overflow(index, Field::UInt(value), ValType::I64))
        }
        Field::I128(value) => {
            i64::try_from(value).map_err(|_| overflow(index, Field::I128(value), ValType::I64))
        }
        Field::U128(value) => {
            i64::try_from(value).map_err(|_| overflow(index, Field::U128(value), ValType::I64))
        }
        Field::Boolean(value) => Ok(if value { 1 } else { 0 }),
        field => Err(Error::InvalidArgumentValue {
            index,
            field,
            wasm_type: ValType::I64,
        }),
    }
}

fn overflow(index: usize, field: Field, wasm_type: ValType) -> Error {
    Error::ArgumentOverflow {
        index,
        field,
        wasm_type,
    }
}

fn wasm_val_to_field(value: Val) -> Field {
    match value {
        Val::I32(value) => Field::Int(i64::from(value)),
        Val::I64(value) => Field::Int(value),
        Val::F32(value) => Field::Float(dozer_types::ordered_float::OrderedFloat(f64::from(
            f32::from_bits(value),
        ))),
        Val::F64(value) => Field::Float(dozer_types::ordered_float::OrderedFloat(f64::from_bits(
            value,
        ))),
        _ => unreachable!("WASM result type validation should reject unsupported return values"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::models::udf_config::{UdfConfig, UdfType, WasmConfig};
    use dozer_types::types::{FieldDefinition, SourceDefinition};
    use std::sync::Arc;

    use crate::builder::ExpressionBuilder;
    use crate::sqlparser::{
        ast::{SelectItem, SetExpr, Statement},
        dialect::DozerDialect,
        parser::Parser,
    };

    const ADD_ONE_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60, 0x01, 0x7e, 0x01,
        0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0b, 0x01, 0x07, 0x61, 0x64, 0x64, 0x5f, 0x6f, 0x6e,
        0x65, 0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x42, 0x01, 0x7c, 0x0b,
    ];

    fn add_one_module_path(name: &str) -> std::path::PathBuf {
        let module_path = std::env::temp_dir().join(name);
        std::fs::write(&module_path, ADD_ONE_WASM).unwrap();
        module_path
    }

    fn test_schema() -> Schema {
        Schema {
            fields: vec![FieldDefinition::new(
                "value".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            )],
            primary_index: vec![],
        }
    }

    #[test]
    fn runs_i64_wasm_udf() {
        let module_path = add_one_module_path("dozer_add_one_test.wasm");
        let schema = test_schema();
        let args = vec![Expression::Column { index: 0 }];
        let (session, return_type) =
            prepare_wasm_session(module_path.to_str().unwrap(), "add_one", &args, &schema).unwrap();
        assert_eq!(FieldType::Int, return_type);

        let mut expression = Expression::WasmUDF {
            name: "add_one".to_string(),
            args,
            return_type,
            session,
        };
        let record = Record::new(vec![Field::Int(41)]);
        let result = expression.evaluate(&record, &schema).unwrap();
        assert_eq!(Field::Int(42), result);
    }

    #[test]
    fn builder_parses_and_runs_wasm_udf() {
        let module_path = add_one_module_path("dozer_add_one_builder_test.wasm");
        let schema = test_schema();
        let udfs = vec![UdfConfig {
            name: "add_one".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: module_path.to_str().unwrap().to_string(),
            }),
        }];

        let ast = Parser::parse_sql(&DozerDialect {}, "SELECT add_one(value)").unwrap();
        let Statement::Query(query) = ast.into_iter().next().unwrap() else {
            panic!("expected query");
        };
        let SetExpr::Select(select) = *query.body else {
            panic!("expected select");
        };
        let SelectItem::UnnamedExpr(sql_expr) = select.projection.into_iter().next().unwrap()
        else {
            panic!("expected expression projection");
        };

        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let mut builder = ExpressionBuilder::new(0, runtime.clone());
        let mut expression = runtime
            .block_on(builder.build(false, &sql_expr, &schema, &udfs))
            .unwrap();
        let record = Record::new(vec![Field::Int(7)]);
        let result = expression.evaluate(&record, &schema).unwrap();
        assert_eq!(Field::Int(8), result);
    }
}
