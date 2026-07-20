use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use dozer_types::{
    ordered_float::OrderedFloat,
    thiserror,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use num_traits::ToPrimitive;
use wasmi::{core::ValueType, Engine, Func, Linker, Module, Store, Value};

use crate::{
    error::Error as ExpressionError,
    execution::{Expression, ExpressionType},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read WASM module '{path}': {source}")]
    ReadModule {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to load WASM module: {0}")]
    LoadModule(#[source] wasmi::Error),
    #[error("failed to instantiate WASM module: {0}")]
    Instantiate(#[source] wasmi::Error),
    #[error("WASM module does not export function '{0}'")]
    MissingExport(String),
    #[error("WASM UDF '{function_name}' must return exactly one value, got {actual}")]
    InvalidResultCount {
        function_name: String,
        actual: usize,
    },
    #[error(
        "invalid argument type for WASM UDF '{function_name}' at index {argument_index}: {actual}"
    )]
    InvalidArgumentType {
        function_name: String,
        argument_index: usize,
        actual: FieldType,
    },
    #[error("invalid return type for WASM UDF '{function_name}': {actual:?}")]
    InvalidReturnType {
        function_name: String,
        actual: ValueType,
    },
    #[error("invalid argument value for WASM UDF '{function_name}' at index {argument_index}: {argument}")]
    InvalidArgumentValue {
        function_name: String,
        argument_index: usize,
        argument: Field,
    },
    #[error("failed to evaluate WASM UDF: {0}")]
    Evaluate(#[source] wasmi::Error),
}

#[derive(Debug, Clone)]
pub struct Udf {
    function_name: String,
    args: Vec<Expression>,
    runtime: Arc<Mutex<WasmRuntime>>,
    params: Vec<ValueType>,
    result: ValueType,
}

#[derive(Debug)]
struct WasmRuntime {
    store: Store<()>,
    function: Func,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.function_name == other.function_name && self.args == other.args
    }
}

impl Udf {
    pub fn new(
        function_name: String,
        path: String,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, ExpressionError> {
        validate_args(&function_name, &args, schema)?;

        let wasm = fs::read(Path::new(&path)).map_err(|source| Error::ReadModule {
            path: path.clone(),
            source,
        })?;
        let engine = Engine::default();
        let module = Module::new(&engine, &mut &wasm[..]).map_err(Error::LoadModule)?;
        let linker = Linker::new(&engine);
        let mut store = Store::new(&engine, ());
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(Error::Instantiate)?
            .start(&mut store)
            .map_err(Error::Instantiate)?;
        let function = instance
            .get_func(&store, &function_name)
            .ok_or_else(|| Error::MissingExport(function_name.clone()))?;
        let ty = function.ty(&store);
        let params = ty.params().to_vec();
        let results = ty.results().to_vec();

        if params.len() != args.len() {
            return Err(ExpressionError::InvalidNumberOfArguments {
                function_name,
                expected: params.len()..(params.len() + 1),
                actual: args.len(),
            });
        }
        if results.len() != 1 {
            return Err(Error::InvalidResultCount {
                function_name,
                actual: results.len(),
            }
            .into());
        }
        let result = results[0];
        let return_type =
            wasm_type_to_field_type(result).ok_or_else(|| Error::InvalidReturnType {
                function_name: function_name.clone(),
                actual: result,
            })?;
        for (index, (arg, param)) in args.iter().zip(params.iter()).enumerate() {
            let typ = arg.get_type(schema)?;
            if !is_compatible_arg_type(typ.return_type, *param) {
                return Err(Error::InvalidArgumentType {
                    function_name,
                    argument_index: index,
                    actual: typ.return_type,
                }
                .into());
            }
        }

        let _ = return_type;
        Ok(Self {
            function_name,
            args,
            runtime: Arc::new(Mutex::new(WasmRuntime { store, function })),
            params,
            result,
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        ExpressionType {
            return_type: wasm_type_to_field_type(self.result).expect("validated return type"),
            nullable: false,
            source: SourceDefinition::Dynamic,
            is_primary_key: false,
        }
    }

    pub fn evaluate(&mut self, record: &Record, schema: &Schema) -> Result<Field, ExpressionError> {
        let inputs = self
            .args
            .iter_mut()
            .zip(self.params.iter())
            .enumerate()
            .map(|(index, (arg, param))| {
                let field = arg.evaluate(record, schema)?;
                field_to_wasm_value(&self.function_name, index, field, *param)
            })
            .collect::<Result<Vec<_>, ExpressionError>>()?;
        let mut outputs = vec![default_output(self.result)];
        let mut runtime = self.runtime.lock().expect("WASM runtime mutex poisoned");
        let WasmRuntime { store, function } = &mut *runtime;
        function
            .call(store, &inputs, &mut outputs)
            .map_err(Error::Evaluate)?;
        Ok(wasm_value_to_field(outputs.remove(0)))
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!(
            "{}({})",
            self.function_name,
            self.args
                .iter()
                .map(|arg| arg.to_string(schema))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

fn validate_args(
    function_name: &str,
    args: &[Expression],
    schema: &Schema,
) -> Result<(), ExpressionError> {
    for (index, arg) in args.iter().enumerate() {
        let typ = arg.get_type(schema)?;
        if !matches!(
            typ.return_type,
            FieldType::Int
                | FieldType::UInt
                | FieldType::Boolean
                | FieldType::Int8
                | FieldType::Float
        ) {
            return Err(Error::InvalidArgumentType {
                function_name: function_name.to_string(),
                argument_index: index,
                actual: typ.return_type,
            }
            .into());
        }
    }
    Ok(())
}

fn is_compatible_arg_type(field_type: FieldType, wasm_type: ValueType) -> bool {
    match field_type {
        FieldType::Int | FieldType::UInt | FieldType::Boolean | FieldType::Int8 => {
            matches!(wasm_type, ValueType::I32 | ValueType::I64)
        }
        FieldType::Float => matches!(wasm_type, ValueType::F32 | ValueType::F64),
        _ => false,
    }
}

fn wasm_type_to_field_type(wasm_type: ValueType) -> Option<FieldType> {
    match wasm_type {
        ValueType::I32 | ValueType::I64 => Some(FieldType::Int),
        ValueType::F32 | ValueType::F64 => Some(FieldType::Float),
        _ => None,
    }
}

fn field_to_wasm_value(
    function_name: &str,
    argument_index: usize,
    field: Field,
    wasm_type: ValueType,
) -> Result<Value, ExpressionError> {
    match wasm_type {
        ValueType::I32 => field
            .to_int()
            .and_then(|value| value.to_i32())
            .map(Value::I32)
            .ok_or_else(|| invalid_argument_value(function_name, argument_index, field)),
        ValueType::I64 => field
            .to_int()
            .map(Value::I64)
            .ok_or_else(|| invalid_argument_value(function_name, argument_index, field)),
        ValueType::F32 => field
            .to_float()
            .and_then(|value| value.to_f32())
            .map(|value| Value::F32(value.into()))
            .ok_or_else(|| invalid_argument_value(function_name, argument_index, field)),
        ValueType::F64 => field
            .to_float()
            .map(|value| Value::F64(value.into()))
            .ok_or_else(|| invalid_argument_value(function_name, argument_index, field)),
        _ => Err(invalid_argument_value(function_name, argument_index, field)),
    }
}

fn invalid_argument_value(
    function_name: &str,
    argument_index: usize,
    argument: Field,
) -> ExpressionError {
    Error::InvalidArgumentValue {
        function_name: function_name.to_string(),
        argument_index,
        argument,
    }
    .into()
}

fn default_output(wasm_type: ValueType) -> Value {
    match wasm_type {
        ValueType::I32 => Value::I32(0),
        ValueType::I64 => Value::I64(0),
        ValueType::F32 => Value::F32(0.0_f32.into()),
        ValueType::F64 => Value::F64(0.0_f64.into()),
        _ => unreachable!("validated return type"),
    }
}

fn wasm_value_to_field(value: Value) -> Field {
    match value {
        Value::I32(value) => Field::Int(i64::from(value)),
        Value::I64(value) => Field::Int(value),
        Value::F32(value) => Field::Float(OrderedFloat::from(f64::from(value.to_float()))),
        Value::F64(value) => Field::Float(OrderedFloat::from(value.to_float())),
        _ => unreachable!("validated return type"),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, sync::Arc};

    use dozer_types::{
        models::udf_config::{UdfConfig, UdfType, WasmConfig},
        types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
    };

    use crate::{builder::ExpressionBuilder, sqlparser::dialect::GenericDialect};

    const ADD_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7e, 0x7e,
        0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00,
        0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x7c, 0x0b,
    ];

    fn write_wasm(name: &str, wasm: &[u8]) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("dozer-wasm-udf-{name}-{}.wasm", std::process::id()));
        fs::write(&path, wasm).unwrap();
        path
    }

    #[test]
    fn evaluates_numeric_wasm_udf() {
        let path = write_wasm("add", ADD_WASM);
        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "left".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "right".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .to_owned();
        let udfs = vec![UdfConfig {
            name: "add".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: path.to_string_lossy().into_owned(),
            }),
        }];
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let expr = sqlparser::parser::Parser::parse_sql(
            &GenericDialect {},
            "SELECT add(left, right) FROM t",
        )
        .unwrap()
        .remove(0);
        let sqlparser::ast::Statement::Query(query) = expr else {
            panic!("expected query")
        };
        let sqlparser::ast::SetExpr::Select(select) = *query.body else {
            panic!("expected select")
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("expected expression")
        };
        let mut builder = ExpressionBuilder::new(schema.fields.len(), runtime.clone());
        let mut expr = runtime
            .block_on(builder.parse_sql_expression(false, expr, &schema, &udfs))
            .unwrap();

        assert_eq!(
            expr.evaluate(&Record::new(vec![Field::Int(2), Field::Int(3)]), &schema)
                .unwrap(),
            Field::Int(5)
        );
        let _ = fs::remove_file(path);
    }
}
