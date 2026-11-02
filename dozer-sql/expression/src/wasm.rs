use std::{fs::File, io::Read, sync::Arc};

use dozer_types::{
    models::udf_config::WasmConfig,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use wasmi::{
    core::ValueType, Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Value,
};

use crate::{
    error::Error,
    execution::{Expression, ExpressionType},
};

const MAX_MODULE_BYTES: u64 = 8 * 1024 * 1024;
const FUEL: u64 = 1_000_000;

fn error(message: impl std::fmt::Display) -> Error {
    Error::Wasm(message.to_string())
}

/// Each evaluation gets fresh isolated memory and globals, including cloned expressions.
#[derive(Debug, Clone)]
pub struct Udf {
    name: String,
    export: String,
    args: Vec<Expression>,
    module: Arc<Module>,
    engine: Engine,
    params: Vec<ValueType>,
    result: ValueType,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.export == other.export
            && self.args == other.args
            && Arc::ptr_eq(&self.module, &other.module)
    }
}

impl Udf {
    pub fn load(
        name: String,
        config: &WasmConfig,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        File::open(&config.path)
            .map_err(error)?
            .take(MAX_MODULE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(error)?;
        Self::from_bytes(
            name.clone(),
            config.export.clone().unwrap_or(name),
            args,
            schema,
            &bytes,
        )
    }

    fn from_bytes(
        name: String,
        export: String,
        args: Vec<Expression>,
        schema: &Schema,
        bytes: &[u8],
    ) -> Result<Self, Error> {
        if bytes.len() as u64 > MAX_MODULE_BYTES {
            return Err(error("module exceeds 8 MiB"));
        }
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, &mut &bytes[..]).map_err(error)?;
        // No WASI, filesystem, network, callbacks, or managed-memory runtime imports.
        if module.imports().next().is_some() {
            return Err(error("imports are not supported"));
        }
        let signature = module
            .get_export(&export)
            .and_then(|e| e.func().cloned())
            .ok_or_else(|| error("export must name a function"))?;
        if signature.params().len() != args.len() || signature.results().len() != 1 {
            return Err(error(
                "expected matching arguments and exactly one scalar result",
            ));
        }
        for (ty, arg) in signature.params().iter().zip(&args) {
            let field = arg.get_type(schema)?.return_type;
            if !matches!(
                (ty, field),
                (ValueType::I32 | ValueType::I64, FieldType::Int)
                    | (ValueType::F32 | ValueType::F64, FieldType::Float)
            ) {
                return Err(error(
                    "integer WASM parameters require SQL Int; float parameters require SQL Float",
                ));
            }
        }
        let result = signature.results()[0];
        if !matches!(
            result,
            ValueType::I32 | ValueType::I64 | ValueType::F32 | ValueType::F64
        ) {
            return Err(error("only scalar numeric results are supported"));
        }
        Ok(Self {
            name,
            export,
            args,
            params: signature.params().to_vec(),
            result,
            module: Arc::new(module),
            engine,
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        ExpressionType::new(
            if matches!(self.result, ValueType::I32 | ValueType::I64) {
                FieldType::Int
            } else {
                FieldType::Float
            },
            true,
            SourceDefinition::Dynamic,
            false,
        )
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!(
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|a| a.to_string(schema))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub fn evaluate(&mut self, record: &Record, schema: &Schema) -> Result<Field, Error> {
        let mut inputs = Vec::new();
        for (arg, ty) in self.args.iter_mut().zip(&self.params) {
            let field = arg.evaluate(record, schema)?;
            if field == Field::Null {
                return Ok(Field::Null);
            }
            inputs.push(match (ty, field) {
                (ValueType::I32, Field::Int(v)) => Value::I32(i32::try_from(v).map_err(error)?),
                (ValueType::I64, Field::Int(v)) => Value::I64(v),
                (ValueType::F32, Field::Float(v))
                    if v.0.is_finite() && (v.0 as f32).is_finite() =>
                {
                    Value::F32((v.0 as f32).into())
                }
                (ValueType::F64, Field::Float(v)) if v.0.is_finite() => Value::F64(v.0.into()),
                _ => return Err(error("invalid numeric argument or integer overflow")),
            });
        }
        let limits = StoreLimitsBuilder::new()
            .memory_size(16 * 1024 * 1024)
            .table_elements(1024)
            .instances(1)
            .memories(1)
            .tables(1)
            .build();
        let mut store: Store<StoreLimits> = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        store.add_fuel(FUEL).map_err(error)?;
        let instance = Linker::<StoreLimits>::new(&self.engine)
            .instantiate(&mut store, &self.module)
            .map_err(error)?
            .start(&mut store)
            .map_err(error)?;
        let function = instance
            .get_func(&store, &self.export)
            .ok_or_else(|| error("missing export"))?;
        let mut outputs = [Value::default(self.result)];
        function
            .call(&mut store, &inputs, &mut outputs)
            .map_err(error)?;
        match &outputs[0] {
            Value::I32(v) => Ok(Field::Int(i64::from(*v))),
            Value::I64(v) => Ok(Field::Int(*v)),
            Value::F32(v) if f32::from(*v).is_finite() => {
                Ok(Field::Float((f32::from(*v) as f64).into()))
            }
            Value::F64(v) if f64::from(*v).is_finite() => Ok(Field::Float(f64::from(*v).into())),
            _ => Err(error("non-finite or unsupported result")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(wat: &str, args: Vec<Expression>) -> Result<Udf, Error> {
        Udf::from_bytes(
            "f".into(),
            "f".into(),
            args,
            &Schema::default(),
            &wat::parse_str(wat).unwrap(),
        )
    }

    #[test]
    fn numeric_export_runs() {
        let mut udf = compile(
            "(module (func (export \"f\") (result i64) i64.const 42))",
            vec![],
        )
        .unwrap();
        assert_eq!(
            udf.evaluate(&Record::new(vec![]), &Schema::default())
                .unwrap(),
            Field::Int(42)
        );
    }

    #[test]
    fn imports_rejected() {
        assert!(compile("(module (import \"env\" \"abort\" (func)) (func (export \"f\") (result i32) i32.const 1))", vec![]).is_err());
    }

    #[test]
    fn infinite_loop_exhausts_fuel() {
        let mut udf = compile(
            "(module (func (export \"f\") (result i64) (loop br 0) i64.const 0))",
            vec![],
        )
        .unwrap();
        assert!(udf
            .evaluate(&Record::new(vec![]), &Schema::default())
            .is_err());
    }

    #[test]
    fn oversized_memory_rejected_at_instantiation() {
        let mut udf = compile(
            "(module (memory 257) (func (export \"f\") (result i32) i32.const 1))",
            vec![],
        )
        .unwrap();
        assert!(udf
            .evaluate(&Record::new(vec![]), &Schema::default())
            .is_err());
    }

    #[test]
    fn bad_signatures_rejected() {
        assert!(compile("(module (func (export \"f\")))", vec![]).is_err());
        assert!(compile(
            "(module (func (export \"f\") (param i32) (result i32) local.get 0))",
            vec![]
        )
        .is_err());
        assert!(Udf::from_bytes(
            "f".into(),
            "f".into(),
            vec![],
            &Schema::default(),
            b"invalid"
        )
        .is_err());
    }

    #[test]
    fn integer_argument_overflow_is_not_truncated() {
        let mut udf = compile(
            "(module (func (export \"f\") (param i32) (result i32) local.get 0))",
            vec![Expression::Literal(Field::Int(i64::MAX))],
        )
        .unwrap();
        assert!(udf
            .evaluate(&Record::new(vec![]), &Schema::default())
            .is_err());
    }

    #[test]
    fn traps_and_non_finite_results_are_errors() {
        for source in [
            "(module (func (export \"f\") (result i32) unreachable))",
            "(module (func (export \"f\") (result f64) f64.const nan))",
        ] {
            let mut udf = compile(source, vec![]).unwrap();
            assert!(udf
                .evaluate(&Record::new(vec![]), &Schema::default())
                .is_err());
        }
    }

    #[test]
    fn globals_are_reset_between_rows_and_clones() {
        let mut udf = compile("(module (global $g (mut i32) (i32.const 0)) (func (export \"f\") (result i32) global.get $g i32.const 1 i32.add global.set $g global.get $g))", vec![]).unwrap();
        let mut cloned = udf.clone();
        for function in [&mut udf, &mut cloned] {
            assert_eq!(
                function
                    .evaluate(&Record::new(vec![]), &Schema::default())
                    .unwrap(),
                Field::Int(1)
            );
            assert_eq!(
                function
                    .evaluate(&Record::new(vec![]), &Schema::default())
                    .unwrap(),
                Field::Int(1)
            );
        }
    }

    #[test]
    fn start_function_shares_fuel_budget() {
        let mut udf = compile("(module (func $start (loop br 0)) (start $start) (func (export \"f\") (result i32) i32.const 1))", vec![]).unwrap();
        assert!(udf
            .evaluate(&Record::new(vec![]), &Schema::default())
            .is_err());
    }

    #[test]
    fn nullable_column_short_circuits() {
        let mut schema = Schema::default();
        schema.fields.push(dozer_types::types::FieldDefinition::new(
            "value".into(),
            FieldType::Int,
            true,
            SourceDefinition::Dynamic,
        ));
        let mut udf = Udf::from_bytes(
            "f".into(),
            "f".into(),
            vec![Expression::Column { index: 0 }],
            &schema,
            &wat::parse_str("(module (func (export \"f\") (param i64) (result i64) local.get 0))")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            udf.evaluate(&Record::new(vec![Field::Null]), &schema)
                .unwrap(),
            Field::Null
        );
    }

    #[test]
    fn assemblyscript_module_via_sql_builder() {
        use crate::builder::ExpressionBuilder;
        use dozer_types::models::udf_config::{UdfConfig, UdfType};
        use sqlparser::{dialect::GenericDialect, parser::Parser};

        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let mut builder = ExpressionBuilder::new(0, runtime.clone());
        let schema = Schema::default();
        for (sql, export, expected) in [
            ("calculate(20,22)", "add", Field::Int(42)),
            ("calculate(1.25)", "twice", Field::Float(2.5.into())),
        ] {
            let config = UdfConfig {
                name: "calculate".into(),
                config: UdfType::Wasm(WasmConfig {
                    path: concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/numeric.wasm")
                        .into(),
                    export: Some(export.into()),
                }),
            };
            let expr = Parser::new(&GenericDialect {})
                .try_with_sql(sql)
                .unwrap()
                .parse_expr()
                .unwrap();
            let mut expression = runtime
                .block_on(builder.build(false, &expr, &schema, &[config]))
                .unwrap();
            assert_eq!(
                expression.evaluate(&Record::new(vec![]), &schema).unwrap(),
                expected
            );
            println!("AssemblyScript export {export}: SQL {sql} => {expected:?} (asserted)");
        }
    }
}
