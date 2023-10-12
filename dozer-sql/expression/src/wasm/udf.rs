use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record, Schema};
use dozer_types::log::warn;
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
    let values: Vec<Val> = input_values
        .iter().zip(param_types)
        .map(|(field, param)| -> Val {match field {
            Field::Int(value) => {
                match param {
                    ValType::I32 => {
                        match i32::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from Int to i32");
                                Val::I32(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::I64 => {
                        Val::I64(*value)
                    },
                    ValType::F32 => todo!(),
                    ValType::F64 => todo!(),
                    ValType::V128 => todo!(),
                    ValType::FuncRef => todo!(),
                    ValType::ExternRef => todo!(),
                }
            },
            Field::Float(value) => {
                match param {
                    ValType::I32 => todo!(),
                    ValType::I64 => todo!(),
                    ValType::F32 => {
                        let value = value.to_bits() as f32;
                        warn!("Precision loss happens due to conversion from f64 to f32");
                        Val::F32(value.to_bits())
                    },
                    ValType::F64 => {
                        Val::F64(value.to_bits())
                    },
                    ValType::V128 => todo!(),
                    ValType::FuncRef => todo!(),
                    ValType::ExternRef => todo!(),
                }
            },
            Field::UInt(value) => {
                match param {
                    ValType::I32 => {
                        match i32::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from u64 to i32");
                                Val::I32(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::I64 => {
                        match i64::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from u64 to i64");
                                Val::I64(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::F32 => todo!(),
                    ValType::F64 => todo!(),
                    ValType::V128 => todo!(),
                    ValType::FuncRef => todo!(),
                    ValType::ExternRef => todo!(),
                }
            },
            Field::U128(value) => {
                match param {
                    ValType::I32 => {
                        match i32::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from u128 to i32");
                                Val::I32(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::I64 => {
                        match i64::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from u128 to i64");
                                Val::I64(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::F32 => todo!(),
                    ValType::F64 => todo!(),
                    ValType::V128 => todo!(),
                    ValType::FuncRef => todo!(),
                    ValType::ExternRef => todo!(),
                }
            },
            Field::I128(value) => {
                match param {
                    ValType::I32 => {
                        match i32::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from i128 to i32");
                                Val::I32(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::I64 => {
                        match i64::try_from(*value) {
                            Ok(value) => {
                                warn!("Precision loss happens due to conversion from i128 to i64");
                                Val::I64(value)
                            },
                            Err(_) => todo!(),
                        }
                    },
                    ValType::F32 => todo!(),
                    ValType::F64 => todo!(),
                    ValType::V128 => todo!(),
                    ValType::FuncRef => todo!(),
                    ValType::ExternRef => todo!(),
                }
            },
            Field::Boolean(_) => todo!(),
            Field::String(_) => todo!(),
            Field::Text(_) => todo!(),
            Field::Binary(_) => todo!(),
            Field::Decimal(_) => todo!(),
            Field::Timestamp(_) => todo!(),
            Field::Date(_) => todo!(),
            Field::Json(_) => todo!(),
            Field::Point(_) => todo!(),
            Field::Duration(_) => todo!(),
            Field::Null => todo!(),
        }})
        .collect();

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
