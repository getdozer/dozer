mod runtime;

use dozer_types::types::{Field, FieldType, Record, Schema, SourceDefinition};
use wasmi::{core::ValueType, Val};

use crate::execution::{Expression, ExpressionType};

pub use runtime::{
    compile_wasm_bytes, load_compiled_module, CompiledWasmModule, Error, WasmDescriptor,
};

#[derive(Debug, Clone)]
pub struct Udf {
    function_name: String,
    args: Vec<Expression>,
    compiled: CompiledWasmModule,
    nullable: bool,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.function_name == other.function_name
            && self.compiled.module_path == other.compiled.module_path
            && self.args == other.args
    }
}

impl Udf {
    pub fn new(
        function_name: String,
        module_path: String,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, crate::error::Error> {
        let compiled = load_compiled_module(WasmDescriptor {
            module_path: &module_path,
            function_name: &function_name,
        })?;
        Self::from_compiled(function_name, compiled, args, schema)
    }

    pub fn from_compiled(
        function_name: String,
        compiled: CompiledWasmModule,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, crate::error::Error> {
        let nullable = validate_args(&function_name, &args, &compiled.param_types, schema)?;
        Ok(Self {
            function_name,
            args,
            compiled,
            nullable,
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        ExpressionType {
            return_type: self.compiled.return_type,
            nullable: self.nullable,
            source: SourceDefinition::Dynamic,
            is_primary_key: false,
        }
    }

    pub fn evaluate(
        &mut self,
        record: &Record,
        schema: &Schema,
    ) -> Result<Field, crate::error::Error> {
        let mut wasm_args = Vec::with_capacity(self.args.len());
        for (idx, expr) in self.args.iter_mut().enumerate() {
            let value = expr.evaluate(record, schema)?;
            if value == Field::Null {
                return Ok(Field::Null);
            }
            let wasm_val = runtime::field_to_wasm_val(
                value,
                self.compiled.param_types[idx],
                &self.function_name,
                idx,
            )?;
            wasm_args.push(wasm_val);
        }

        let (mut store, func) = runtime::instantiate_function(
            &self.compiled.engine,
            &self.compiled.module,
            WasmDescriptor {
                module_path: &self.compiled.module_path,
                function_name: &self.function_name,
            },
        )?;
        let mut results = [Val::default(self.compiled.wasm_return_type)];
        func.call(&mut store, &wasm_args, &mut results)
            .map_err(|source| Error::WasmEngine {
                module: self.compiled.module_path.clone(),
                source,
            })?;
        Ok(runtime::wasm_val_to_field(&results[0], &self.function_name)?)
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        let rendered_args = self
            .args
            .iter()
            .map(|e| e.to_string(schema))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}({})", self.function_name, rendered_args)
    }
}

fn validate_args(
    function_name: &str,
    args: &[Expression],
    param_types: &[ValueType],
    schema: &Schema,
) -> Result<bool, crate::error::Error> {
    if args.len() != param_types.len() {
        let expected_len = param_types.len();
        return Err(crate::error::Error::InvalidNumberOfArguments {
            function_name: function_name.to_string(),
            expected: expected_len..expected_len + 1,
            actual: args.len(),
        });
    }
    let mut any_nullable = false;
    for (idx, (arg, param_type)) in args.iter().zip(param_types.iter()).enumerate() {
        let expr_type = arg.get_type(schema)?;
        any_nullable |= expr_type.nullable;
        let expected = expected_field_types(*param_type, function_name)?;
        if !expected.contains(&expr_type.return_type) {
            return Err(crate::error::Error::InvalidFunctionArgumentType {
                function_name: function_name.to_string(),
                argument_index: idx,
                expected,
                actual: expr_type.return_type,
            });
        }
    }
    Ok(any_nullable)
}

fn expected_field_types(
    param_type: ValueType,
    function_name: &str,
) -> Result<Vec<FieldType>, crate::error::Error> {
    match param_type {
        ValueType::I32 | ValueType::I64 => {
            Ok(vec![FieldType::Int, FieldType::UInt, FieldType::Boolean])
        }
        ValueType::F32 | ValueType::F64 => {
            Ok(vec![FieldType::Float, FieldType::Int, FieldType::UInt])
        }
        other => Err(Error::UnsupportedValueType(other, function_name.to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::types::FieldDefinition;

    // Minimal WebAssembly module exporting `add_i64(i64, i64) -> i64`.
    const ADD_I64_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7e, 0x7e,
        0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0b, 0x01, 0x07, 0x61, 0x64, 0x64, 0x5f, 0x69,
        0x36, 0x34, 0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x7c, 0x0b,
    ];

    #[test]
    fn test_wasm_udf_evaluation_and_null_propagation() {
        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "a".to_string(),
                    FieldType::Int,
                    true,
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

        let compiled = compile_wasm_bytes(
            WasmDescriptor {
                module_path: "in_memory.wasm",
                function_name: "add_i64",
            },
            ADD_I64_WASM,
        )
        .unwrap();
        let mut udf = Udf::from_compiled(
            "add_i64".to_string(),
            compiled,
            vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 1 },
            ],
            &schema,
        )
        .unwrap();

        assert_eq!(udf.get_type().return_type, FieldType::Int);
        assert!(udf.get_type().nullable);

        let record = Record::new(vec![Field::Int(15), Field::Int(27)]);
        assert_eq!(udf.evaluate(&record, &schema).unwrap(), Field::Int(42));

        let null_record = Record::new(vec![Field::Null, Field::Int(27)]);
        assert_eq!(udf.evaluate(&null_record, &schema).unwrap(), Field::Null);
    }
}
