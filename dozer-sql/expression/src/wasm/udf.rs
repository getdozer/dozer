use super::error::Error::{WasmFunctionMissing, WasmTrap};
use crate::error::Error::{self, Wasm};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record, Schema};

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
            return Err(Wasm(WasmFunctionMissing(
                name.to_string(),
                config.to_string(),
            )));
        }
    }

    let func_type = wasm_udf_func.ty(&mut store);
    let param_types = func_type.params();
    let mut result_type = func_type.results();

    // Parse the types
    // There are a lot of unwraps() and panics, because there is type checking done before
    let values: Vec<Val> = input_values
        .iter()
        .zip(param_types)
        .map(|(field, param)| -> Val {
            match field {
                Field::Int(value) => match param {
                    ValType::I32 => Val::I32(i32::try_from(*value).unwrap()),
                    ValType::I64 => Val::I64(*value),
                    _ => panic!("Wasm type checking failed"),
                },
                Field::Float(value) => match param {
                    ValType::F32 => Val::F32((value.to_bits() as f32).to_bits()),
                    ValType::F64 => Val::F64(value.to_bits()),
                    _ => panic!("Wasm type checking failed"),
                },
                Field::UInt(value) => match param {
                    ValType::I32 => Val::I32(i32::try_from(*value).unwrap()),
                    ValType::I64 => Val::I64(i64::try_from(*value).unwrap()),
                    _ => panic!("Wasm type checking failed"),
                },
                Field::U128(value) => match param {
                    ValType::I32 => Val::I32(i32::try_from(*value).unwrap()),
                    ValType::I64 => Val::I64(i64::try_from(*value).unwrap()),
                    _ => panic!("Wasm type checking failed"),
                },
                Field::I128(value) => match param {
                    ValType::I32 => Val::I32(i32::try_from(*value).unwrap()),
                    ValType::I64 => Val::I64(i64::try_from(*value).unwrap()),
                    _ => panic!("Wasm type checking failed"),
                },
                _ => panic!("Wasm type checking failed"),
            }
        })
        .collect();

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
        _ => panic!("Wasm type checking failed"),
    })
}
