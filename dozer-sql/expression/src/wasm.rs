//! Scalar WebAssembly UDFs. Modules are compiled once; each row gets fresh instance state.

use std::sync::Arc;

use dozer_types::{
    models::udf_config::WasmConfig,
    ordered_float::OrderedFloat,
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

const FUEL_PER_CALL: u64 = 10_000_000;
const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
struct CompiledModule {
    engine: Engine,
    module: Module,
}

#[derive(Clone, Debug)]
pub struct Udf {
    name: String,
    path: String,
    export: String,
    args: Vec<Expression>,
    compiled: Arc<CompiledModule>,
    params: Vec<ValueType>,
    result: ValueType,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.path == other.path
            && self.export == other.export
            && self.args == other.args
    }
}

fn wasm_error(error: impl std::fmt::Display) -> Error {
    Error::Wasm(error.to_string())
}

fn argument_types(ty: ValueType) -> Vec<FieldType> {
    match ty {
        ValueType::I32 | ValueType::I64 => vec![
            FieldType::Int8,
            FieldType::Int,
            FieldType::UInt,
            FieldType::I128,
            FieldType::U128,
            FieldType::Boolean,
        ],
        ValueType::F32 | ValueType::F64 => vec![FieldType::Float],
        _ => vec![],
    }
}

impl Udf {
    pub fn new(name: String, config: &WasmConfig, args: Vec<Expression>) -> Result<Self, Error> {
        let bytes = std::fs::read(&config.path)
            .map_err(|error| wasm_error(format!("cannot read {}: {error}", config.path)))?;
        let mut engine_config = Config::default();
        engine_config.consume_fuel(true);
        let engine = Engine::new(&engine_config);
        let module = Module::new(&engine, &mut &bytes[..]).map_err(wasm_error)?;
        let export = config.function.clone().unwrap_or_else(|| name.clone());
        let function_type = match module.get_export(&export) {
            Some(ExternType::Func(ty)) => ty,
            _ => return Err(wasm_error(format!("{export} is not an exported function"))),
        };
        let params = function_type.params().to_vec();
        if params.len() != args.len() {
            return Err(Error::InvalidNumberOfArguments {
                function_name: name,
                expected: params.len()..params.len() + 1,
                actual: args.len(),
            });
        }
        let result = match function_type.results() {
            [ty @ (ValueType::I32 | ValueType::I64 | ValueType::F32 | ValueType::F64)] => *ty,
            _ => {
                return Err(wasm_error(
                    "a scalar UDF must return exactly one numeric value",
                ))
            }
        };
        for ty in &params {
            if argument_types(*ty).is_empty() {
                return Err(wasm_error(format!(
                    "unsupported WASM parameter type {ty:?}"
                )));
            }
        }
        let udf = Self {
            name,
            path: config.path.clone(),
            export,
            args,
            compiled: Arc::new(CompiledModule { engine, module }),
            params,
            result,
        };
        // Reject unsupported imports, oversized memory and failing start functions at planning time.
        udf.instantiate()?;
        Ok(udf)
    }

    fn instantiate(&self) -> Result<(Store<StoreLimits>, Instance), Error> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(MEMORY_LIMIT_BYTES)
            .memories(1)
            .table_elements(10_000)
            .tables(1)
            .instances(1)
            .build();
        let mut store = Store::new(&self.compiled.engine, limits);
        store.limiter(|limits| limits);
        store.add_fuel(FUEL_PER_CALL).map_err(wasm_error)?;
        let mut linker = Linker::new(&self.compiled.engine);
        // AssemblyScript uses this import for assertions/runtime checks. It is not a host I/O API.
        linker
            .func_wrap(
                "env",
                "abort",
                |_message: i32, _file: i32, line: i32, column: i32| -> Result<(), Trap> {
                    Err(Trap::new(format!(
                        "AssemblyScript abort at {line}:{column}"
                    )))
                },
            )
            .map_err(wasm_error)?;
        let instance = linker
            .instantiate(&mut store, &self.compiled.module)
            .map_err(wasm_error)?
            .start(&mut store)
            .map_err(wasm_error)?;
        Ok((store, instance))
    }

    pub fn get_type(&self, schema: &Schema) -> Result<ExpressionType, Error> {
        // Aggregate columns are appended by the planner after expression construction.
        let mut nullable = false;
        for (index, (arg, ty)) in self.args.iter().zip(&self.params).enumerate() {
            if matches!(arg, Expression::Literal(Field::Null)) {
                nullable = true;
                continue;
            }
            let argument_type = arg.get_type(schema)?;
            nullable |= argument_type.nullable;
            let expected = argument_types(*ty);
            if !expected.contains(&argument_type.return_type) {
                return Err(Error::InvalidFunctionArgumentType {
                    function_name: self.name.clone(),
                    argument_index: index,
                    expected,
                    actual: argument_type.return_type,
                });
            }
        }
        Ok(ExpressionType {
            return_type: match self.result {
                ValueType::I32 | ValueType::I64 => FieldType::Int,
                _ => FieldType::Float,
            },
            nullable,
            source: SourceDefinition::Dynamic,
            is_primary_key: false,
        })
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
        let values = fields
            .into_iter()
            .zip(&self.params)
            .enumerate()
            .map(|(index, (field, ty))| to_wasm_value(field, *ty, &self.name, index))
            .collect::<Result<Vec<_>, _>>()?;
        let (mut store, instance) = self.instantiate()?;
        let function = instance
            .get_func(&store, &self.export)
            .ok_or_else(|| wasm_error("configured function export disappeared"))?;
        let mut output = [Value::default(self.result)];
        function
            .call(&mut store, &values, &mut output)
            .map_err(wasm_error)?;
        match output[0] {
            Value::I32(value) => Ok(Field::Int(value.into())),
            Value::I64(value) => Ok(Field::Int(value)),
            Value::F32(value) => Ok(Field::Float(OrderedFloat(f32::from(value).into()))),
            Value::F64(value) => Ok(Field::Float(OrderedFloat(f64::from(value)))),
            _ => Err(wasm_error("unsupported WASM result")),
        }
    }
}

fn to_wasm_value(
    field: Field,
    ty: ValueType,
    function: &str,
    index: usize,
) -> Result<Value, Error> {
    let integer = match &field {
        Field::Int8(value) => Some(i128::from(*value)),
        Field::Int(value) => Some(i128::from(*value)),
        Field::UInt(value) => Some(i128::from(*value)),
        Field::I128(value) => Some(*value),
        Field::U128(value) => i128::try_from(*value).ok(),
        Field::Boolean(value) => Some(i128::from(*value)),
        _ => None,
    };
    let value = match (ty, &field) {
        (ValueType::I32, _) => integer.and_then(|v| i32::try_from(v).ok()).map(Value::I32),
        (ValueType::I64, _) => integer.and_then(|v| i64::try_from(v).ok()).map(Value::I64),
        (ValueType::F32, Field::Float(value)) => {
            let converted = value.0 as f32;
            if value.0.is_finite() && !converted.is_finite() {
                None
            } else {
                Some(Value::F32(converted.into()))
            }
        }
        (ValueType::F64, Field::Float(value)) => Some(Value::F64(value.0.into())),
        _ => None,
    };
    value.ok_or_else(|| Error::InvalidFunctionArgument {
        function_name: function.to_owned(),
        argument_index: index,
        argument: field,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn udf(wat: &str, args: Vec<Expression>) -> Result<Udf, Error> {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), wat::parse_str(wat).unwrap()).unwrap();
        let function = Udf::new(
            "test".into(),
            &WasmConfig {
                path: file.path().to_str().unwrap().into(),
                function: Some("run".into()),
            },
            args,
        )?;
        function.get_type(&Schema::default())?;
        Ok(function)
    }

    fn evaluate(udf: &mut Udf) -> Result<Field, Error> {
        udf.evaluate(&Record::new(vec![]), &Schema::default())
    }

    #[test]
    fn numeric_exports_and_checked_integer_conversions() {
        for (ty, field, expected) in [
            ("i32", Field::Int8(i8::MIN), Field::Int(i8::MIN.into())),
            ("i64", Field::Int8(i8::MAX), Field::Int(i8::MAX.into())),
            ("i32", Field::Int(42), Field::Int(42)),
            ("i64", Field::UInt(42), Field::Int(42)),
            ("i32", Field::Boolean(true), Field::Int(1)),
            (
                "f32",
                Field::Float(OrderedFloat(1.25)),
                Field::Float(OrderedFloat(1.25)),
            ),
            (
                "f64",
                Field::Float(OrderedFloat(2.5)),
                Field::Float(OrderedFloat(2.5)),
            ),
        ] {
            let code =
                format!("(module (func (export \"run\") (param {ty}) (result {ty}) local.get 0))");
            let mut function = udf(&code, vec![Expression::Literal(field)]).unwrap();
            assert_eq!(evaluate(&mut function).unwrap(), expected);
        }
        for field in [
            Field::Int(i64::MAX),
            Field::UInt(u64::MAX),
            Field::U128(u128::MAX),
        ] {
            let mut function = udf(
                "(module (func (export \"run\") (param i32) (result i32) local.get 0))",
                vec![Expression::Literal(field)],
            )
            .unwrap();
            assert!(matches!(
                evaluate(&mut function),
                Err(Error::InvalidFunctionArgument { .. })
            ));
        }
    }

    #[test]
    fn null_skips_execution_and_preserves_nullability() {
        let mut function = udf(
            "(module (func (export \"run\") (param i64) (result i64) unreachable))",
            vec![Expression::Literal(Field::Null)],
        )
        .unwrap();
        assert!(function.get_type(&Schema::default()).unwrap().nullable);
        assert_eq!(evaluate(&mut function).unwrap(), Field::Null);
    }

    #[test]
    fn validates_arity_types_exports_and_results() {
        assert!(udf(
            "(module (func (export \"run\") (param i64) (result i64) local.get 0))",
            vec![]
        )
        .is_err());
        assert!(udf(
            "(module (func (export \"run\") (param i64) (result i64) local.get 0))",
            vec![Expression::Literal(Field::String("no".into()))]
        )
        .is_err());
        assert!(udf("(module (memory (export \"run\") 1))", vec![]).is_err());
        assert!(udf("(module (func (export \"run\")))", vec![]).is_err());
        assert!(udf(
            "(module (func (export \"run\") (result i32 i32) i32.const 1 i32.const 2))",
            vec![]
        )
        .is_err());
        assert!(udf(
            "(module (func (export \"run\") (param externref) (result i32) i32.const 1))",
            vec![Expression::Literal(Field::Null)]
        )
        .is_err());
    }

    #[test]
    fn bounds_execution_and_memory_and_reports_traps() {
        let mut loop_forever = udf(
            "(module (func (export \"run\") (result i32) (loop br 0) i32.const 0))",
            vec![],
        )
        .unwrap();
        assert!(evaluate(&mut loop_forever).is_err());
        let mut trap = udf(
            "(module (func (export \"run\") (result i32) unreachable))",
            vec![],
        )
        .unwrap();
        assert!(evaluate(&mut trap).is_err());
        assert!(udf(
            "(module (memory 1025) (func (export \"run\") (result i32) i32.const 0))",
            vec![]
        )
        .is_err());
        assert!(udf(
            "(module (table 10001 funcref) (func (export \"run\") (result i32) i32.const 0))",
            vec![]
        )
        .is_err());
    }

    #[test]
    fn unsupported_imports_and_failing_start_functions_are_rejected() {
        assert!(udf("(module (import \"env\" \"read_file\" (func)) (func (export \"run\") (result i32) i32.const 0))", vec![]).is_err());
        assert!(udf("(module (func $start unreachable) (start $start) (func (export \"run\") (result i32) i32.const 0))", vec![]).is_err());
        assert!(udf("(module (func $start (loop br 0)) (start $start) (func (export \"run\") (result i32) i32.const 0))", vec![]).is_err());
    }

    #[test]
    fn overflowing_float_argument_is_rejected() {
        let mut function = udf(
            "(module (func (export \"run\") (param f32) (result f32) local.get 0))",
            vec![Expression::Literal(Field::Float(OrderedFloat(f64::MAX)))],
        )
        .unwrap();
        assert!(matches!(
            evaluate(&mut function),
            Err(Error::InvalidFunctionArgument { .. })
        ));
    }

    #[test]
    fn each_record_has_fresh_module_state() {
        let mut function = udf("(module (global $counter (mut i32) (i32.const 0)) (func (export \"run\") (result i32) global.get $counter i32.const 1 i32.add global.set $counter global.get $counter))", vec![]).unwrap();
        assert_eq!(evaluate(&mut function).unwrap(), Field::Int(1));
        assert_eq!(evaluate(&mut function).unwrap(), Field::Int(1));
    }

    #[test]
    fn assemblyscript_abort_is_reported_without_host_io() {
        let mut function = udf("(module (import \"env\" \"abort\" (func $abort (param i32 i32 i32 i32))) (func (export \"run\") (result i32) i32.const 0 i32.const 0 i32.const 9 i32.const 3 call $abort i32.const 0))", vec![]).unwrap();
        assert!(evaluate(&mut function)
            .unwrap_err()
            .to_string()
            .contains("AssemblyScript abort at 9:3"));
    }
}
