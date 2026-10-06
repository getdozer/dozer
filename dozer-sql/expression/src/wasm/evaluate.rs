use std::{
    collections::BTreeMap,
    num::NonZeroI32,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use deno_core::{error::AnyError, Extension};
use dozer_types::{
    json_types::{field_to_json_value, DestructuredJson},
    ordered_float::OrderedFloat,
    serde_json, thiserror,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use tokio::{runtime::Runtime, sync::Mutex};

use crate::execution::{Expression, ExpressionType};

use super::binary::{parse_module_bytes, FuncSignature, ParsedModule, ValType};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read wasm module: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Module(#[from] super::binary::Error),
    #[error("failed to create wasm udf runtime: {0}")]
    CreateRuntime(#[from] dozer_deno::RuntimeError),
    #[error("failed to evaluate wasm udf: {0}")]
    Evaluate(#[source] AnyError),
    #[error("failed to generate wasm udf module: {0}")]
    SerdeJson(#[from] dozer_types::serde_json::Error),
    #[error("unsupported wasm udf signature: {0}")]
    UnsupportedSignature(String),
    #[error("unexpected wasm udf result: {0}")]
    UnexpectedResult(String),
}

pub fn parse_module(path: &str) -> Result<ParsedModule, Error> {
    let bytes = std::fs::read(path)?;
    let info = parse_module_bytes(&bytes)?;
    Ok(ParsedModule { bytes, info })
}

#[derive(Debug, Clone)]
pub struct Udf {
    function_name: String,
    args: Vec<Expression>,
    tokio_runtime: Arc<Runtime>,
    /// `Arc<Mutex>` to enable `Clone`. Not sure why `Expression` should be `Clone`.
    deno_runtime: Arc<Mutex<dozer_deno::Runtime>>,
    function: NonZeroI32,
    return_type: FieldType,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        // This is obviously wrong. We have to lift the `PartialEq` constraint.
        self.function_name == other.function_name && self.args == other.args
    }
}

impl Udf {
    pub async fn new(
        tokio_runtime: Arc<Runtime>,
        function_name: String,
        module: ParsedModule,
        signature: FuncSignature,
        args: Vec<Expression>,
    ) -> Result<Self, Error> {
        if signature.results.len() != 1 {
            return Err(Error::UnsupportedSignature(format!(
                "function {function_name} must return exactly one value"
            )));
        }
        let return_type = match signature.results[0] {
            ValType::I32 | ValType::I64 => FieldType::Int,
            ValType::F32 | ValType::F64 => FieldType::Float,
            ValType::Unsupported(byte) => {
                return Err(Error::UnsupportedSignature(format!(
                    "unsupported value type: 0x{byte:02x}"
                )))
            }
        };

        let source = generate_module_source(&module, &function_name, &signature)?;
        let path = write_temp_module(&source)?;
        let created = dozer_deno::Runtime::new(
            vec![path.to_string_lossy().into_owned()],
            Vec::<fn() -> Extension>::new(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
        let (deno_runtime, functions) = created?;
        let function = *functions
            .first()
            .ok_or_else(|| Error::UnexpectedResult("module has no default export".to_string()))?;

        Ok(Self {
            function_name,
            args,
            tokio_runtime,
            deno_runtime: Arc::new(Mutex::new(deno_runtime)),
            function,
            return_type,
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
        self.tokio_runtime.block_on(evaluate_impl(
            self.function_name.clone(),
            &mut self.args,
            self.return_type,
            &self.deno_runtime,
            self.function,
            record,
            schema,
        ))
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!(
            "{}({})",
            self.function_name,
            self.args
                .iter()
                .map(|arg| arg.to_string(schema))
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

async fn evaluate_impl(
    function_name: String,
    args: &mut [Expression],
    return_type: FieldType,
    runtime: &Arc<Mutex<dozer_deno::Runtime>>,
    function: NonZeroI32,
    record: &Record,
    schema: &Schema,
) -> Result<Field, crate::error::Error> {
    let mut json_args = Vec::with_capacity(args.len());
    for (index, arg) in args.iter_mut().enumerate() {
        let field = arg.evaluate(record, schema)?;
        match field {
            Field::Int(_)
            | Field::UInt(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::Decimal(_) => json_args.push(field_to_json_value(field)),
            argument => {
                return Err(crate::error::Error::InvalidFunctionArgument {
                    function_name,
                    argument_index: index,
                    argument,
                })
            }
        }
    }

    let mut runtime = runtime.lock().await;
    let result = runtime
        .call_function(function, json_args)
        .await
        .map_err(Error::Evaluate)?;

    match result.destructure() {
        DestructuredJson::Number(number) => {
            let value = number.to_f64().unwrap_or(0.0);
            Ok(match return_type {
                FieldType::Int => Field::Int(value.round() as i64),
                _ => Field::Float(OrderedFloat(value)),
            })
        }
        DestructuredJson::Null => Ok(Field::Null),
        _ => Err(crate::error::Error::Wasm(Error::UnexpectedResult(
            "wasm udf returned a non-numeric value".to_string(),
        ))),
    }
}

fn generate_module_source(
    module: &ParsedModule,
    function_name: &str,
    signature: &FuncSignature,
) -> Result<String, Error> {
    let bytes_literal = serde_json::to_string(&base64_encode(&module.bytes))?;
    let imports = generate_imports(module)?;

    let mut body = String::new();
    for (index, param) in signature.params.iter().enumerate() {
        if *param == ValType::I64 {
            body.push_str(&format!(
                "const __arg{index} = BigInt(Math.trunc(Number(__args[{index}])) || 0);\n"
            ));
        } else {
            body.push_str(&format!("const __arg{index} = __args[{index}];\n"));
        }
    }
    let call_args = (0..signature.params.len())
        .map(|index| format!("__arg{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let export_literal = serde_json::to_string(function_name)?;
    if signature.results.iter().all(ValType::is_integer) {
        body.push_str(&format!(
            "return Number(__instance.exports[{export_literal}]({call_args}));\n"
        ));
    } else {
        body.push_str(&format!(
            "return __instance.exports[{export_literal}]({call_args});\n"
        ));
    }

    Ok(format!(
        "const __bytes = Uint8Array.from(atob({bytes_literal}), (c) => c.charCodeAt(0));\n\
         const __module = new WebAssembly.Module(__bytes);\n\
         const __instance = new WebAssembly.Instance(__module, {imports});\n\
         export default async function (...__args) {{\n\
         {body}\
         }}\n"
    ))
}

fn generate_imports(module: &ParsedModule) -> Result<String, Error> {
    let mut imports_by_module: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for import in &module.info.imported_functions {
        imports_by_module
            .entry(&import.module)
            .or_default()
            .push(&import.field);
    }

    let mut imports = String::from("{");
    for (module_name, fields) in imports_by_module {
        let module_literal = serde_json::to_string(module_name)?;
        imports.push_str(&format!("{module_literal}: {{"));
        for field in fields {
            let field_literal = serde_json::to_string(field)?;
            let message_literal = serde_json::to_string(&format!(
                "imported function {module_name}.{field} is not supported"
            ))?;
            imports.push_str(&format!(
                "{field_literal}: () => {{ throw new Error({message_literal}); }},"
            ));
        }
        imports.push_str("},");
    }
    imports.push('}');
    Ok(imports)
}

static TEMP_MODULE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn write_temp_module(source: &str) -> Result<std::path::PathBuf, Error> {
    let counter = TEMP_MODULE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "dozer_wasm_udf_{}_{}.js",
        std::process::id(),
        counter
    ));
    std::fs::write(&path, source)?;
    Ok(path)
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let value = (b0 << 16) | (b1 << 8) | b2;
        result.push(ALPHABET[(value >> 18) as usize & 63] as char);
        result.push(ALPHABET[(value >> 12) as usize & 63] as char);
        result.push(if chunk.len() > 1 {
            ALPHABET[(value >> 6) as usize & 63] as char
        } else {
            '='
        });
        result.push(if chunk.len() > 2 {
            ALPHABET[value as usize & 63] as char
        } else {
            '='
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_udf(export_name: &str, module: ParsedModule, args: Vec<Expression>) -> Field {
        let signature = module.info.exported_function_signature(export_name).unwrap();
        let build_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let eval_runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap(),
        );
        let mut udf = build_runtime
            .block_on(Udf::new(
                eval_runtime,
                export_name.to_string(),
                module,
                signature,
                args,
            ))
            .unwrap();
        let schema = Schema::new();
        let record = Record::new(vec![]);
        udf.evaluate(&record, &schema).unwrap()
    }

    #[test]
    fn evaluates_f64_udf() {
        let bytes = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7c,
            0x7c, 0x01, 0x7c, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64,
            0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0xa0, 0x0b,
        ];
        let info = parse_module_bytes(&bytes).unwrap();
        let module = ParsedModule { bytes, info };
        let result = run_udf(
            "add",
            module,
            vec![
                Expression::Literal(Field::Float(OrderedFloat(1.5))),
                Expression::Literal(Field::Float(OrderedFloat(2.25))),
            ],
        );
        assert_eq!(result, Field::Float(OrderedFloat(3.75)));
    }

    #[test]
    fn evaluates_i32_udf() {
        let bytes = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7f,
            0x7f, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x6d, 0x75, 0x6c,
            0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6c, 0x0b,
        ];
        let info = parse_module_bytes(&bytes).unwrap();
        let module = ParsedModule { bytes, info };
        let result = run_udf(
            "mul",
            module,
            vec![
                Expression::Literal(Field::Int(3)),
                Expression::Literal(Field::Int(4)),
            ],
        );
        assert_eq!(result, Field::Int(12));
    }
}
