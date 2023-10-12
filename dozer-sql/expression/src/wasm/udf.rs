use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, Record, Schema};
use super::error::Error::{WasmInputDataTypeMismatchErr, WasmUnsupportedReturnType,
    WasmUnsupportedInputType, WasmTrap, WasmUnsupportedReturnTypeSize, WasmInputTypeSizeMismatch,
    WasmFunctionMissing};
use crate::error::Error::{self, Wasm};

use wasmtime::*;

use crate::execution::Expression;

pub fn evaluate_wasm_udf(
    schema: &Schema,
    name: &str,
    config: &str,
    args: &[Expression],
    record: &Record,
) -> Result<Field, Error> {
    let input_values = args
        .iter()
        .map(|arg| arg.evaluate(record, schema))
        .collect::<Result<Vec<_>, Error>>()?;

    let values: Vec<Val> = input_values
        .iter()
        .map(|field| match field {
            Field::Int(value) => Val::I64(*value),
            Field::Float(value) => Val::F64(value.to_bits()),
            _ => todo!(),
        })
        .collect();

    let engine = Engine::default();
    let module = Module::from_file(&engine, config).unwrap();
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[]).unwrap();

    let wasm_udf_func;
    match instance.get_func(&mut store, name) {
        Some(func) => {
            wasm_udf_func = func;
        }
        None => {
            return Err(Wasm(WasmFunctionMissing(name.to_string(), config.to_string())));
        },
    }

    let func_type = wasm_udf_func.ty(&mut store);
    let param_types = func_type.params();
    let mut result_type = func_type.results();

    // Validation
    // 1. `args` received must be equal to the params we expect
    if param_types.len() != args.len() {
        return Err(Wasm(WasmInputTypeSizeMismatch(param_types.len(), args.len())));
    }
    // 2. Types must also agree
    for (param, expr) in param_types.zip(args) {
        match expr.get_type(schema) {
            Ok(ty) => {
                match ty.return_type {
                    FieldType::UInt |
                    FieldType::U128 => return Err(Wasm(WasmUnsupportedInputType(ty.return_type.to_string()))),
                    FieldType::Int => {
                        match param {
                            ValType::I32 |
                            ValType::I64 => {

                            },
                            ValType::F32 |
                            ValType::F64 |
                            ValType::V128 |
                            ValType::FuncRef |
                            ValType::ExternRef => return Err(Wasm(WasmInputDataTypeMismatchErr(param.to_string(), ty.return_type.to_string()))),
                        }
                    },
                    FieldType::I128 => return Err(Wasm(WasmUnsupportedInputType(ty.return_type.to_string()))),
                    FieldType::Float => {
                        match param {
                            ValType::F32 |
                            ValType::F64 => {

                            },
                            ValType::I32 |
                            ValType::I64 |
                            ValType::V128 |
                            ValType::FuncRef |
                            ValType::ExternRef => return Err(Wasm(WasmInputDataTypeMismatchErr(param.to_string(), ty.return_type.to_string()))),
                        }
                    },
                    FieldType::Boolean |
                    FieldType::String |
                    FieldType::Text |
                    FieldType::Binary |
                    FieldType::Decimal |
                    FieldType::Timestamp |
                    FieldType::Date |
                    FieldType::Json |
                    FieldType::Point |
                    FieldType::Duration => {
                        return Err(Wasm(WasmUnsupportedInputType(ty.return_type.to_string())));
                    }
                }
            },
            Err(_) => todo!(),
        }
    }

    // 3. Return type must also agree
    if result_type.len() != 1 {
        return Err(Wasm(WasmUnsupportedReturnTypeSize(result_type.len())));
    }

    let result = result_type.next().unwrap();

    let mut results: [Val; 1] = [Val::I64(0)];

    match wasm_udf_func.call(&mut store, &values, &mut results) {
        Ok(()) => {}
        Err(trap) => {
            return Err(Wasm(WasmTrap(name.to_string(), trap.to_string())));
        }
    }

    Ok(match result {
        ValType::I32 => Field::Int(i64::from(results[0].i32().unwrap())),
        ValType::I64 => Field::Int(results[0].i64().unwrap()),
        ValType::F32 => Field::Float(OrderedFloat(f64::from(results[0].f32().unwrap()))),
        ValType::F64 => Field::Float(OrderedFloat(results[0].f64().unwrap())),
        ValType::V128 |
        ValType::FuncRef |
        ValType::ExternRef => {
            return Err(Wasm(WasmUnsupportedReturnType(result.to_string())));
        },
    })
}
