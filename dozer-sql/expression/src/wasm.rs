use std::{fmt, path::Path, sync::Arc};

use dozer_types::{
    ordered_float::OrderedFloat,
    thiserror,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use wasmtime::{Engine, ExternType, FuncType, Instance, Module, Store, Val, ValType};

use crate::{
    error::Error as ExpressionError,
    execution::{Expression, ExpressionType},
};

pub struct Udf {
    name: String,
    args: Vec<Expression>,
    session: Arc<Session>,
    return_type: ExpressionType,
}

impl Clone for Udf {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            args: self.args.clone(),
            session: self.session.clone(),
            return_type: self.get_type(),
        }
    }
}

impl fmt::Debug for Udf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Udf")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("session", &self.session)
            .field("return_type", &self.return_type.return_type)
            .field("nullable", &self.return_type.nullable)
            .finish()
    }
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.args == other.args
            && self.session == other.session
            && self.return_type.return_type == other.return_type.return_type
            && self.return_type.nullable == other.return_type.nullable
    }
}

impl Udf {
    pub fn new(
        name: String,
        module_path: String,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, ExpressionError> {
        let session = Session::new(module_path, name.clone())?;
        session.validate_args(&args, schema)?;

        let nullable = args
            .iter()
            .map(|arg| arg.get_type(schema))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|typ| typ.nullable);
        let return_type = ExpressionType::new(
            session.return_type,
            nullable,
            SourceDefinition::Dynamic,
            false,
        );

        Ok(Self {
            name,
            args,
            session: Arc::new(session),
            return_type,
        })
    }

    pub fn evaluate(&mut self, record: &Record, schema: &Schema) -> Result<Field, ExpressionError> {
        let mut values = Vec::with_capacity(self.args.len());
        for (index, (arg, wasm_type)) in self
            .args
            .iter_mut()
            .zip(self.session.params.iter())
            .enumerate()
        {
            let field = arg.evaluate(record, schema)?;
            if field == Field::Null {
                return Ok(Field::Null);
            }
            values.push(field_to_wasm_val(index, field, *wasm_type)?);
        }

        self.session.call(&values).map_err(Into::into)
    }

    pub fn get_type(&self) -> ExpressionType {
        ExpressionType::new(
            self.return_type.return_type,
            self.return_type.nullable,
            self.return_type.source.clone(),
            self.return_type.is_primary_key,
        )
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
}

#[derive(Clone)]
struct Session {
    module_path: String,
    function_name: String,
    engine: Engine,
    module: Module,
    params: Vec<ValType>,
    result: ValType,
    return_type: FieldType,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("module_path", &self.module_path)
            .field("function_name", &self.function_name)
            .field("params", &self.params)
            .field("result", &self.result)
            .finish()
    }
}

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        self.module_path == other.module_path
            && self.function_name == other.function_name
            && self.params == other.params
            && self.result == other.result
    }
}

impl Session {
    fn new(module_path: String, function_name: String) -> Result<Self, Error> {
        let engine = Engine::default();
        let module = Module::from_file(&engine, Path::new(&module_path)).map_err(|source| {
            Error::LoadModule {
                path: module_path.clone(),
                source,
            }
        })?;
        let function_type = exported_function_type(&module, &module_path, &function_name)?;
        let params = function_type.params().collect::<Vec<_>>();
        let results = function_type.results().collect::<Vec<_>>();

        if results.len() != 1 {
            return Err(Error::InvalidResultCount {
                function: function_name,
                actual: results.len(),
            });
        }

        let result = results[0];
        let return_type =
            field_type_from_wasm_result(result).ok_or_else(|| Error::UnsupportedResultType {
                function: function_name.clone(),
                wasm_type: result,
            })?;

        Ok(Self {
            module_path,
            function_name,
            engine,
            module,
            params,
            result,
            return_type,
        })
    }

    fn validate_args(&self, args: &[Expression], schema: &Schema) -> Result<(), ExpressionError> {
        if self.params.len() != args.len() {
            return Err(Error::InvalidArgumentCount {
                function: self.function_name.clone(),
                expected: self.params.len(),
                actual: args.len(),
            }
            .into());
        }

        for (index, (arg, wasm_type)) in args.iter().zip(self.params.iter()).enumerate() {
            let ExpressionType { return_type, .. } = arg.get_type(schema)?;
            if !can_pass_as_wasm(return_type, *wasm_type) {
                return Err(Error::UnsupportedArgumentType {
                    function: self.function_name.clone(),
                    index,
                    field_type: return_type,
                    wasm_type: *wasm_type,
                }
                .into());
            }
        }
        Ok(())
    }

    fn call(&self, values: &[Val]) -> Result<Field, Error> {
        let mut store = Store::new(&self.engine, ());
        let instance =
            Instance::new(&mut store, &self.module, &[]).map_err(|source| Error::Instantiate {
                path: self.module_path.clone(),
                source,
            })?;
        let function = instance
            .get_func(&mut store, &self.function_name)
            .ok_or_else(|| Error::MissingFunction {
                path: self.module_path.clone(),
                function: self.function_name.clone(),
            })?;

        let mut results = vec![zero_val(self.result)];
        function
            .call(&mut store, values, &mut results)
            .map_err(|source| Error::Trap {
                function: self.function_name.clone(),
                source,
            })?;
        wasm_val_to_field(results.remove(0))
    }
}

fn exported_function_type(
    module: &Module,
    module_path: &str,
    function_name: &str,
) -> Result<FuncType, Error> {
    for export in module.exports() {
        if export.name() == function_name {
            return match export.ty() {
                ExternType::Func(function_type) => Ok(function_type),
                _ => Err(Error::ExportIsNotFunction {
                    path: module_path.to_string(),
                    function: function_name.to_string(),
                }),
            };
        }
    }

    Err(Error::MissingFunction {
        path: module_path.to_string(),
        function: function_name.to_string(),
    })
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

fn field_type_from_wasm_result(wasm_type: ValType) -> Option<FieldType> {
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

fn zero_val(wasm_type: ValType) -> Val {
    match wasm_type {
        ValType::I32 => Val::I32(0),
        ValType::I64 => Val::I64(0),
        ValType::F32 => Val::F32(0),
        ValType::F64 => Val::F64(0),
        _ => unreachable!("unsupported result types are rejected during validation"),
    }
}

fn wasm_val_to_field(value: Val) -> Result<Field, Error> {
    match value {
        Val::I32(value) => Ok(Field::Int(i64::from(value))),
        Val::I64(value) => Ok(Field::Int(value)),
        Val::F32(value) => Ok(Field::Float(OrderedFloat(f64::from(f32::from_bits(value))))),
        Val::F64(value) => Ok(Field::Float(OrderedFloat(f64::from_bits(value)))),
        value => Err(Error::UnsupportedResultValue { value }),
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
    #[error("WASM export {function} in module {path} is not a function")]
    ExportIsNotFunction { path: String, function: String },
    #[error("WASM function {function} must return exactly one value, got {actual}")]
    InvalidResultCount { function: String, actual: usize },
    #[error("WASM function {function} expected {expected} arguments, got {actual}")]
    InvalidArgumentCount {
        function: String,
        expected: usize,
        actual: usize,
    },
    #[error(
        "WASM function {function} argument {index} cannot accept Dozer type {field_type:?} as {wasm_type:?}"
    )]
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
    #[error("WASM returned unsupported value {value:?}")]
    UnsupportedResultValue { value: Val },
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use dozer_types::{
        models::udf_config::{UdfConfig, UdfType, WasmConfig},
        types::{FieldDefinition, SourceDefinition},
    };

    use super::*;
    use crate::{
        builder::ExpressionBuilder,
        sqlparser::{
            ast::{SelectItem, SetExpr, Statement},
            dialect::DozerDialect,
            parser::Parser,
        },
    };

    static MODULE_ID: AtomicUsize = AtomicUsize::new(0);

    fn write_wat(name: &str, wat: &str) -> PathBuf {
        let id = MODULE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("dozer_{name}_{id}.wasm"));
        fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
        path
    }

    fn schema(fields: &[(&str, FieldType, bool)]) -> Schema {
        let mut schema = Schema::default();
        for (name, typ, nullable) in fields {
            schema.field(
                FieldDefinition::new(
                    (*name).to_string(),
                    *typ,
                    *nullable,
                    SourceDefinition::Dynamic,
                ),
                false,
            );
        }
        schema
    }

    fn build_projection_expr(sql: &str) -> crate::sqlparser::ast::Expr {
        let ast = Parser::parse_sql(&DozerDialect {}, sql).unwrap();
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
        sql_expr
    }

    #[test]
    fn builder_parses_and_runs_i64_wasm_udf() {
        let module_path = write_wat(
            "add_one",
            r#"
            (module
              (func (export "add_one") (param i64) (result i64)
                local.get 0
                i64.const 1
                i64.add))
            "#,
        );
        let schema = schema(&[("value", FieldType::Int, false)]);
        let udfs = vec![UdfConfig {
            name: "add_one".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: module_path.to_str().unwrap().to_string(),
            }),
        }];

        let runtime = std::sync::Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let sql_expr = build_projection_expr("SELECT add_one(value)");
        let mut builder = ExpressionBuilder::new(0, runtime.clone());
        let mut expression = runtime
            .block_on(builder.build(false, &sql_expr, &schema, &udfs))
            .unwrap();

        assert_eq!(
            FieldType::Int,
            expression.get_type(&schema).unwrap().return_type
        );
        assert_eq!(
            Field::Int(42),
            expression
                .evaluate(&Record::new(vec![Field::Int(41)]), &schema)
                .unwrap()
        );
    }

    #[test]
    fn validates_function_arity_before_execution() {
        let module_path = write_wat(
            "add_two",
            r#"
            (module
              (func (export "add_two") (param i64 i64) (result i64)
                local.get 0
                local.get 1
                i64.add))
            "#,
        );
        let schema = schema(&[("value", FieldType::Int, false)]);
        let result = Udf::new(
            "add_two".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![Expression::Column { index: 0 }],
            &schema,
        );

        assert!(matches!(
            result,
            Err(ExpressionError::Wasm(Error::InvalidArgumentCount {
                expected: 2,
                actual: 1,
                ..
            }))
        ));
    }

    #[test]
    fn rejects_unsupported_argument_types() {
        let module_path = write_wat(
            "strlen",
            r#"
            (module
              (func (export "strlen") (param i32) (result i32)
                local.get 0))
            "#,
        );
        let schema = schema(&[("name", FieldType::String, false)]);
        let result = Udf::new(
            "strlen".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![Expression::Column { index: 0 }],
            &schema,
        );

        assert!(matches!(
            result,
            Err(ExpressionError::Wasm(Error::UnsupportedArgumentType {
                field_type: FieldType::String,
                ..
            }))
        ));
    }

    #[test]
    fn rejects_multi_value_results() {
        let module_path = write_wat(
            "pair",
            r#"
            (module
              (func (export "pair") (result i64 i64)
                i64.const 1
                i64.const 2))
            "#,
        );
        let schema = schema(&[]);
        let result = Udf::new(
            "pair".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![],
            &schema,
        );

        assert!(matches!(
            result,
            Err(ExpressionError::Wasm(Error::InvalidResultCount {
                actual: 2,
                ..
            }))
        ));
    }

    #[test]
    fn rejects_non_function_exports() {
        let module_path = write_wat(
            "lookup",
            r#"
            (module
              (memory (export "lookup") 1))
            "#,
        );
        let schema = schema(&[]);
        let result = Udf::new(
            "lookup".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![],
            &schema,
        );

        assert!(matches!(
            result,
            Err(ExpressionError::Wasm(Error::ExportIsNotFunction { .. }))
        ));
    }

    #[test]
    fn checks_i32_overflow_at_runtime() {
        let module_path = write_wat(
            "identity_i32",
            r#"
            (module
              (func (export "identity_i32") (param i32) (result i32)
                local.get 0))
            "#,
        );
        let schema = schema(&[("value", FieldType::Int, false)]);
        let mut udf = Udf::new(
            "identity_i32".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![Expression::Column { index: 0 }],
            &schema,
        )
        .unwrap();

        let result = udf.evaluate(
            &Record::new(vec![Field::Int(i64::from(i32::MAX) + 1)]),
            &schema,
        );
        assert!(matches!(
            result,
            Err(ExpressionError::Wasm(Error::ArgumentOverflow { .. }))
        ));
    }

    #[test]
    fn returns_null_without_calling_wasm_when_any_argument_is_null() {
        let module_path = write_wat(
            "trap_if_called",
            r#"
            (module
              (func (export "trap_if_called") (param i64) (result i64)
                unreachable))
            "#,
        );
        let schema = schema(&[("value", FieldType::Int, true)]);
        let mut udf = Udf::new(
            "trap_if_called".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![Expression::Column { index: 0 }],
            &schema,
        )
        .unwrap();

        let result = udf
            .evaluate(&Record::new(vec![Field::Null]), &schema)
            .unwrap();
        assert_eq!(Field::Null, result);
        assert!(udf.get_type().nullable);
    }

    #[test]
    fn supports_f64_arguments_and_results() {
        let module_path = write_wat(
            "halve",
            r#"
            (module
              (func (export "halve") (param f64) (result f64)
                local.get 0
                f64.const 2
                f64.div))
            "#,
        );
        let schema = schema(&[("value", FieldType::Float, false)]);
        let mut udf = Udf::new(
            "halve".to_string(),
            module_path.to_str().unwrap().to_string(),
            vec![Expression::Column { index: 0 }],
            &schema,
        )
        .unwrap();

        let result = udf
            .evaluate(&Record::new(vec![Field::Float(OrderedFloat(9.0))]), &schema)
            .unwrap();
        assert_eq!(Field::Float(OrderedFloat(4.5)), result);
    }
}
