use std::{fs, sync::Arc};

use dozer_types::{
    ordered_float::OrderedFloat,
    thiserror,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use wasmi::{Engine, Extern, Linker, Module, Store, Val, ValType, F64};

use crate::execution::{Expression, ExpressionType};

#[derive(Debug, Clone)]
pub struct Udf {
    udf_name: String,
    function_name: String,
    return_type: FieldType,
    args: Vec<Expression>,
    engine: Engine,
    module: Arc<Module>,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.udf_name == other.udf_name
            && self.function_name == other.function_name
            && self.return_type == other.return_type
            && self.args == other.args
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read WASM module {path}: {source}")]
    ReadModule {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to load WASM module: {0}")]
    LoadModule(String),
    #[error("failed to instantiate WASM module: {0}")]
    Instantiate(String),
    #[error("WASM function {0} was not found")]
    MissingFunction(String),
    #[error("WASM export {0} is not a function")]
    ExportNotFunction(String),
    #[error("WASM function {function_name} expected {expected} arguments, got {actual}")]
    InvalidArgumentCount {
        function_name: String,
        expected: usize,
        actual: usize,
    },
    #[error("Unsupported WASM argument type for function {function_name}: {field_type}")]
    UnsupportedArgumentType {
        function_name: String,
        field_type: FieldType,
    },
    #[error("Unsupported WASM return type for function {function_name}: {field_type}")]
    UnsupportedReturnType {
        function_name: String,
        field_type: FieldType,
    },
    #[error("Invalid null argument for WASM function {function_name}")]
    NullArgument { function_name: String },
    #[error(
        "WASM function {function_name} expected {expected:?}, got {actual:?} at argument {index}"
    )]
    InvalidArgumentType {
        function_name: String,
        index: usize,
        expected: ValType,
        actual: ValType,
    },
    #[error("WASM function {function_name} expected return {expected:?}, got {actual:?}")]
    InvalidReturnType {
        function_name: String,
        expected: ValType,
        actual: ValType,
    },
    #[error("WASM function {function_name} expected one return value, got {actual}")]
    InvalidReturnCount {
        function_name: String,
        actual: usize,
    },
    #[error("failed to call WASM function {function_name}: {source}")]
    Call {
        function_name: String,
        source: wasmi::Error,
    },
}

impl Udf {
    pub fn new(
        udf_name: String,
        module_path: String,
        function_name: Option<String>,
        return_type: FieldType,
        args: Vec<Expression>,
    ) -> Result<Self, Error> {
        let engine = Engine::default();
        let module_bytes = fs::read(&module_path).map_err(|source| Error::ReadModule {
            path: module_path,
            source,
        })?;
        let module = Module::new(&engine, &module_bytes[..])
            .map_err(|error| Error::LoadModule(error.to_string()))?;

        let function_name = function_name.unwrap_or_else(|| udf_name.clone());
        field_type_to_wasm_type(&function_name, return_type)?;
        Ok(Self {
            udf_name,
            function_name,
            return_type,
            args,
            engine,
            module: Arc::new(module),
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        ExpressionType {
            return_type: self.return_type,
            nullable: false,
            source: SourceDefinition::Dynamic,
            is_primary_key: false,
        }
    }

    pub fn evaluate(
        &mut self,
        record: &Record,
        schema: &Schema,
    ) -> Result<Field, crate::error::Error> {
        let mut store = Store::new(&self.engine, ());
        let linker = Linker::new(&self.engine);
        let instance = linker
            .instantiate_and_start(&mut store, &self.module)
            .map_err(|error| Error::Instantiate(error.to_string()))?;
        let export = instance
            .get_export(&store, &self.function_name)
            .ok_or_else(|| Error::MissingFunction(self.function_name.clone()))?;
        let Extern::Func(func) = export else {
            return Err(Error::ExportNotFunction(self.function_name.clone()).into());
        };

        let func_type = func.ty(&store);
        let param_types = func_type.params().to_vec();
        if param_types.len() != self.args.len() {
            return Err(Error::InvalidArgumentCount {
                function_name: self.function_name.clone(),
                expected: param_types.len(),
                actual: self.args.len(),
            }
            .into());
        }

        let mut params = Vec::with_capacity(self.args.len());
        for (index, arg) in self.args.iter_mut().enumerate() {
            let field = arg.evaluate(record, schema)?;
            let value = field_to_wasm_value(&self.function_name, field)?;
            if value.ty() != param_types[index] {
                return Err(Error::InvalidArgumentType {
                    function_name: self.function_name.clone(),
                    index,
                    expected: param_types[index],
                    actual: value.ty(),
                }
                .into());
            }
            params.push(value);
        }

        let return_types = func_type.results().to_vec();
        if return_types.len() != 1 {
            return Err(Error::InvalidReturnCount {
                function_name: self.function_name.clone(),
                actual: return_types.len(),
            }
            .into());
        }

        let expected_return_type = field_type_to_wasm_type(&self.function_name, self.return_type)?;
        if return_types[0] != expected_return_type {
            return Err(Error::InvalidReturnType {
                function_name: self.function_name.clone(),
                expected: expected_return_type,
                actual: return_types[0],
            }
            .into());
        }

        let mut results = vec![Val::default(expected_return_type)];
        func.call(&mut store, &params, &mut results)
            .map_err(|source| Error::Call {
                function_name: self.function_name.clone(),
                source,
            })?;

        wasm_value_to_field(&self.function_name, self.return_type, results.remove(0))
            .map_err(Into::into)
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!(
            "{}({})",
            self.udf_name,
            self.args
                .iter()
                .map(|expr| expr.to_string(schema))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

fn field_to_wasm_value(function_name: &str, field: Field) -> Result<Val, Error> {
    match field {
        Field::Int(value) => Ok(Val::I64(value)),
        Field::UInt(value) => Ok(Val::I64(value as i64)),
        Field::Float(value) => Ok(Val::F64(F64::from(value.0))),
        Field::Boolean(value) => Ok(Val::I32(i32::from(value))),
        Field::Null => Err(Error::NullArgument {
            function_name: function_name.to_string(),
        }),
        field => Err(Error::UnsupportedArgumentType {
            function_name: function_name.to_string(),
            field_type: field.ty().unwrap_or(FieldType::String),
        }),
    }
}

fn field_type_to_wasm_type(function_name: &str, field_type: FieldType) -> Result<ValType, Error> {
    match field_type {
        FieldType::Int | FieldType::UInt => Ok(ValType::I64),
        FieldType::Float => Ok(ValType::F64),
        FieldType::Boolean => Ok(ValType::I32),
        field_type => Err(Error::UnsupportedReturnType {
            function_name: function_name.to_string(),
            field_type,
        }),
    }
}

fn wasm_value_to_field(
    function_name: &str,
    return_type: FieldType,
    value: Val,
) -> Result<Field, Error> {
    match (return_type, value) {
        (FieldType::Int, Val::I64(value)) => Ok(Field::Int(value)),
        (FieldType::UInt, Val::I64(value)) => Ok(Field::UInt(value as u64)),
        (FieldType::Float, Val::F64(value)) => Ok(Field::Float(OrderedFloat(value.to_float()))),
        (FieldType::Boolean, Val::I32(value)) => Ok(Field::Boolean(value != 0)),
        (return_type, value) => Err(Error::InvalidReturnType {
            function_name: function_name.to_string(),
            expected: field_type_to_wasm_type(function_name, return_type)?,
            actual: value.ty(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::types::{FieldDefinition, Schema};

    fn write_wasm_module(name: &str, wat_source: &str) -> String {
        let wasm = wat::parse_str(wat_source).unwrap();
        let path =
            std::env::temp_dir().join(format!("dozer-wasm-udf-{name}-{}.wasm", std::process::id()));
        std::fs::write(&path, wasm).unwrap();
        path.to_string_lossy().to_string()
    }

    fn schema(field_type: FieldType) -> Schema {
        let mut schema = Schema::new();
        schema.field(
            FieldDefinition::new(
                "value".to_string(),
                field_type,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        );
        schema
    }

    #[test]
    fn evaluates_i64_udf() {
        let module = write_wasm_module(
            "add-one",
            r#"
            (module
              (func (export "add_one") (param i64) (result i64)
                local.get 0
                i64.const 1
                i64.add))
            "#,
        );
        let mut udf = Udf::new(
            "add_one".to_string(),
            module,
            None,
            FieldType::Int,
            vec![Expression::Column { index: 0 }],
        )
        .unwrap();
        let schema = schema(FieldType::Int);
        let record = Record::new(vec![Field::Int(41)]);

        assert_eq!(udf.evaluate(&record, &schema).unwrap(), Field::Int(42));
    }

    #[test]
    fn evaluates_boolean_udf() {
        let module = write_wasm_module(
            "not",
            r#"
            (module
              (func (export "not_value") (param i32) (result i32)
                local.get 0
                i32.eqz))
            "#,
        );
        let mut udf = Udf::new(
            "not_value".to_string(),
            module,
            None,
            FieldType::Boolean,
            vec![Expression::Column { index: 0 }],
        )
        .unwrap();
        let schema = schema(FieldType::Boolean);
        let record = Record::new(vec![Field::Boolean(true)]);

        assert_eq!(
            udf.evaluate(&record, &schema).unwrap(),
            Field::Boolean(false)
        );
    }

    #[test]
    fn rejects_unsupported_return_type() {
        let module = write_wasm_module(
            "string-return",
            r#"
            (module
              (func (export "return_string") (result i32)
                i32.const 0))
            "#,
        );
        let error = Udf::new(
            "return_string".to_string(),
            module,
            None,
            FieldType::String,
            vec![],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            Error::UnsupportedReturnType {
                field_type: FieldType::String,
                ..
            }
        ));
    }
}
