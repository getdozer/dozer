mod binary;
mod evaluate;

pub use binary::{parse_module_bytes, FuncSignature, ModuleInfo, ParsedModule, ValType};
pub use evaluate::{parse_module, Error, Udf};
