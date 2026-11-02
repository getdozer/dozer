use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dozer_types::{
    ordered_float::OrderedFloat,
    thiserror::{self, Error},
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};

use crate::execution::{Expression, ExpressionType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValType {
    I32,
    I64,
    F32,
    F64,
}

impl ValType {
    fn from_byte(b: u8) -> Result<Self, Error> {
        match b {
            0x7F => Ok(Self::I32),
            0x7E => Ok(Self::I64),
            0x7D => Ok(Self::F32),
            0x7C => Ok(Self::F64),
            other => Err(Error::InvalidModule(format!(
                "unsupported value type byte: 0x{other:02x}"
            ))),
        }
    }

    pub fn to_field_type(self) -> FieldType {
        match self {
            Self::I32 | Self::I64 => FieldType::Int,
            Self::F32 | Self::F64 => FieldType::Float,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WasmVal {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

impl WasmVal {
    fn default_for(ty: ValType) -> Self {
        match ty {
            ValType::I32 => Self::I32(0),
            ValType::I64 => Self::I64(0),
            ValType::F32 => Self::F32(0.0),
            ValType::F64 => Self::F64(0.0),
        }
    }

    fn as_i32(self) -> Result<i32, Error> {
        match self {
            Self::I32(v) => Ok(v),
            other => Err(Error::Trap(format!("expected i32 on stack, got {other:?}"))),
        }
    }

    fn as_i64(self) -> Result<i64, Error> {
        match self {
            Self::I64(v) => Ok(v),
            other => Err(Error::Trap(format!("expected i64 on stack, got {other:?}"))),
        }
    }

    fn as_f32(self) -> Result<f32, Error> {
        match self {
            Self::F32(v) => Ok(v),
            other => Err(Error::Trap(format!("expected f32 on stack, got {other:?}"))),
        }
    }

    fn as_f64(self) -> Result<f64, Error> {
        match self {
            Self::F64(v) => Ok(v),
            other => Err(Error::Trap(format!("expected f64 on stack, got {other:?}"))),
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to load WASM module at '{path}': {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid WASM module: {0}")]
    InvalidModule(String),
    #[error("exported function '{function_name}' not found in WASM module '{path}'")]
    ExportNotFound { function_name: String, path: String },
    #[error("WASM execution trap in '{0}'")]
    Trap(String),
}

#[derive(Debug, Clone)]
struct FuncType {
    params: Vec<ValType>,
    results: Vec<ValType>,
}

#[derive(Debug, Clone)]
struct FunctionBody {
    locals: Vec<ValType>,
    code: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WasmModule {
    types: Vec<FuncType>,
    func_type_indices: Vec<usize>,
    exports: Vec<(String, usize)>,
    bodies: Vec<FunctionBody>,
    initial_memory: Vec<u8>,
    max_memory_pages: Option<usize>,
    initial_globals: Vec<WasmVal>,
}

#[derive(Debug, Clone)]
pub struct Udf {
    function_name: String,
    export_name: String,
    module_path: String,
    module: Arc<WasmModule>,
    func_idx: usize,
    param_types: Vec<ValType>,
    return_type: FieldType,
    args: Vec<Expression>,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        self.function_name == other.function_name
            && self.export_name == other.export_name
            && self.module_path == other.module_path
            && self.args == other.args
    }
}

pub fn resolve_wasm_path(raw_path: &str) -> PathBuf {
    let candidate = Path::new(raw_path);
    if candidate.exists() {
        return candidate.to_path_buf();
    }
    if candidate.extension().is_none() {
        let with_ext = candidate.with_extension("wasm");
        if with_ext.exists() {
            return with_ext;
        }
        return with_ext;
    }
    candidate.to_path_buf()
}

impl Udf {
    pub fn new(
        function_name: String,
        config: &dozer_types::models::udf_config::WasmConfig,
        args: Vec<Expression>,
        schema: &Schema,
    ) -> Result<Self, crate::error::Error> {
        let resolved_path = resolve_wasm_path(&config.path);
        let resolved_str = resolved_path.to_string_lossy().to_string();
        let bytes = fs::read(&resolved_path).map_err(|source| {
            crate::error::Error::Wasm(Error::Io {
                path: resolved_str.clone(),
                source,
            })
        })?;

        let module = Arc::new(WasmModule::parse(&bytes).map_err(crate::error::Error::Wasm)?);
        let export_name = config
            .function
            .clone()
            .unwrap_or_else(|| function_name.clone());

        let func_idx = module
            .find_export(&export_name)
            .or_else(|| {
                module
                    .exports
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&export_name))
                    .map(|(_, idx)| *idx)
            })
            .ok_or_else(|| {
                crate::error::Error::Wasm(Error::ExportNotFound {
                    function_name: export_name.clone(),
                    path: resolved_str.clone(),
                })
            })?;

        let sig = module
            .func_signature(func_idx)
            .map_err(crate::error::Error::Wasm)?
            .clone();

        if args.len() != sig.params.len() {
            return Err(crate::error::Error::InvalidNumberOfArguments {
                function_name,
                expected: sig.params.len()..(sig.params.len() + 1),
                actual: args.len(),
            });
        }

        for (idx, expr) in args.iter().enumerate() {
            let expr_type = expr.get_type(schema)?;
            let allowed = matches!(
                expr_type.return_type,
                FieldType::Int
                    | FieldType::UInt
                    | FieldType::Int8
                    | FieldType::I128
                    | FieldType::U128
                    | FieldType::Float
                    | FieldType::Decimal
                    | FieldType::Boolean
            );
            if !allowed {
                return Err(crate::error::Error::InvalidFunctionArgumentType {
                    function_name,
                    argument_index: idx,
                    expected: vec![FieldType::Int, FieldType::UInt, FieldType::Float],
                    actual: expr_type.return_type,
                });
            }
        }

        let return_type = sig
            .results
            .first()
            .copied()
            .map(ValType::to_field_type)
            .unwrap_or(FieldType::Int);

        Ok(Self {
            function_name,
            export_name,
            module_path: resolved_str,
            module,
            func_idx,
            param_types: sig.params.clone(),
            return_type,
            args,
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        let nullable = true;
        ExpressionType::new(
            self.return_type,
            nullable,
            SourceDefinition::Dynamic,
            false,
        )
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

    pub fn evaluate(&mut self, record: &Record, schema: &Schema) -> Result<Field, crate::error::Error> {
        let mut wasm_args = Vec::with_capacity(self.args.len());
        for (idx, arg_expr) in self.args.iter_mut().enumerate() {
            let val = arg_expr.evaluate(record, schema)?;
            if matches!(val, Field::Null) {
                return Ok(Field::Null);
            }
            let target_ty = self.param_types[idx];
            let wasm_val = field_to_wasm_val(&self.function_name, idx, val, target_ty)?;
            wasm_args.push(wasm_val);
        }

        let result = self
            .module
            .invoke(self.func_idx, &wasm_args)
            .map_err(crate::error::Error::Wasm)?;

        Ok(match result {
            Some(WasmVal::I32(v)) => Field::Int(i64::from(v)),
            Some(WasmVal::I64(v)) => Field::Int(v),
            Some(WasmVal::F32(v)) => Field::Float(OrderedFloat(f64::from(v))),
            Some(WasmVal::F64(v)) => Field::Float(OrderedFloat(v)),
            None => Field::Null,
        })
    }
}

fn field_to_wasm_val(
    function_name: &str,
    argument_index: usize,
    field: Field,
    target: ValType,
) -> Result<WasmVal, crate::error::Error> {
    use dozer_types::rust_decimal::prelude::ToPrimitive;

    match target {
        ValType::I32 => {
            let v = match field {
                Field::Int(n) => n as i32,
                Field::UInt(n) => n as i32,
                Field::Int8(n) => i32::from(n),
                Field::I128(n) => n as i32,
                Field::U128(n) => n as i32,
                Field::Float(OrderedFloat(f)) => f as i32,
                Field::Decimal(d) => d.to_i32().unwrap_or(0),
                Field::Boolean(b) => i32::from(b),
                other => {
                    return Err(crate::error::Error::InvalidFunctionArgument {
                        function_name: function_name.to_string(),
                        argument_index,
                        argument: other,
                    })
                }
            };
            Ok(WasmVal::I32(v))
        }
        ValType::I64 => {
            let v = match field {
                Field::Int(n) => n,
                Field::UInt(n) => n as i64,
                Field::Int8(n) => i64::from(n),
                Field::I128(n) => n as i64,
                Field::U128(n) => n as i64,
                Field::Float(OrderedFloat(f)) => f as i64,
                Field::Decimal(d) => d.to_i64().unwrap_or(0),
                Field::Boolean(b) => i64::from(b),
                other => {
                    return Err(crate::error::Error::InvalidFunctionArgument {
                        function_name: function_name.to_string(),
                        argument_index,
                        argument: other,
                    })
                }
            };
            Ok(WasmVal::I64(v))
        }
        ValType::F32 => {
            let v = match field {
                Field::Float(OrderedFloat(f)) => f as f32,
                Field::Int(n) => n as f32,
                Field::UInt(n) => n as f32,
                Field::Int8(n) => f32::from(n),
                Field::I128(n) => n as f32,
                Field::U128(n) => n as f32,
                Field::Decimal(d) => d.to_f32().unwrap_or(0.0),
                Field::Boolean(b) => {
                    if b {
                        1.0
                    } else {
                        0.0
                    }
                }
                other => {
                    return Err(crate::error::Error::InvalidFunctionArgument {
                        function_name: function_name.to_string(),
                        argument_index,
                        argument: other,
                    })
                }
            };
            Ok(WasmVal::F32(v))
        }
        ValType::F64 => {
            let v = match field {
                Field::Float(OrderedFloat(f)) => f,
                Field::Int(n) => n as f64,
                Field::UInt(n) => n as f64,
                Field::Int8(n) => f64::from(n),
                Field::I128(n) => n as f64,
                Field::U128(n) => n as f64,
                Field::Decimal(d) => d.to_f64().unwrap_or(0.0),
                Field::Boolean(b) => {
                    if b {
                        1.0
                    } else {
                        0.0
                    }
                }
                other => {
                    return Err(crate::error::Error::InvalidFunctionArgument {
                        function_name: function_name.to_string(),
                        argument_index,
                        argument: other,
                    })
                }
            };
            Ok(WasmVal::F64(v))
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        if self.pos >= self.bytes.len() {
            return Err(Error::InvalidModule("unexpected EOF".to_string()));
        }
        let b = self.bytes[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], Error> {
        if self.pos + len > self.bytes.len() {
            return Err(Error::InvalidModule("unexpected EOF reading slice".to_string()));
        }
        let s = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        Ok(s)
    }

    fn read_u32_leb(&mut self) -> Result<u32, Error> {
        let mut result = 0u32;
        let mut shift = 0;
        loop {
            let byte = self.read_u8()?;
            result |= u32::from(byte & 0x7F)
                .checked_shl(shift)
                .ok_or_else(|| Error::InvalidModule("LEB128 u32 overflow".to_string()))?;
            if (byte & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 35 {
                return Err(Error::InvalidModule("LEB128 u32 too many bytes".to_string()));
            }
        }
    }

    fn read_i32_leb(&mut self) -> Result<i32, Error> {
        let mut result = 0i32;
        let mut shift = 0;
        let mut byte;
        loop {
            byte = self.read_u8()?;
            result |= i32::from(byte & 0x7F) << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
            if shift >= 35 {
                return Err(Error::InvalidModule("LEB128 i32 too many bytes".to_string()));
            }
        }
        if shift < 32 && (byte & 0x40) != 0 {
            result |= !0 << shift;
        }
        Ok(result)
    }

    fn read_i64_leb(&mut self) -> Result<i64, Error> {
        let mut result = 0i64;
        let mut shift = 0;
        let mut byte;
        loop {
            byte = self.read_u8()?;
            result |= i64::from(byte & 0x7F) << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
            if shift >= 70 {
                return Err(Error::InvalidModule("LEB128 i64 too many bytes".to_string()));
            }
        }
        if shift < 64 && (byte & 0x40) != 0 {
            result |= !0i64 << shift;
        }
        Ok(result)
    }

    fn read_f32(&mut self) -> Result<f32, Error> {
        let b = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_f64(&mut self) -> Result<f64, Error> {
        let b = self.read_bytes(8)?;
        Ok(f64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn read_string(&mut self) -> Result<String, Error> {
        let len = self.read_u32_leb()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::InvalidModule(format!("invalid UTF-8 identifier: {e}")))
    }
}

impl WasmModule {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 8 || &bytes[0..4] != b"\0asm" || &bytes[4..8] != &[1, 0, 0, 0] {
            return Err(Error::InvalidModule(
                "missing or invalid WASM magic/version header".to_string(),
            ));
        }

        let mut cursor = Cursor::new(&bytes[8..]);
        let mut types = Vec::new();
        let mut func_type_indices = Vec::new();
        let mut exports = Vec::new();
        let mut bodies = Vec::new();
        let mut initial_memory = Vec::new();
        let mut max_memory_pages = None;
        let mut initial_globals = Vec::new();

        while !cursor.is_empty() {
            let section_id = cursor.read_u8()?;
            let section_len = cursor.read_u32_leb()? as usize;
            let section_bytes = cursor.read_bytes(section_len)?;
            let mut sec = Cursor::new(section_bytes);

            match section_id {
                1 => {
                    // Type section
                    let count = sec.read_u32_leb()? as usize;
                    for _ in 0..count {
                        let form = sec.read_u8()?;
                        if form != 0x60 {
                            return Err(Error::InvalidModule(format!(
                                "unsupported type form: 0x{form:02x}"
                            )));
                        }
                        let param_count = sec.read_u32_leb()? as usize;
                        let mut params = Vec::with_capacity(param_count);
                        for _ in 0..param_count {
                            params.push(ValType::from_byte(sec.read_u8()?)?);
                        }
                        let result_count = sec.read_u32_leb()? as usize;
                        let mut results = Vec::with_capacity(result_count);
                        for _ in 0..result_count {
                            results.push(ValType::from_byte(sec.read_u8()?)?);
                        }
                        types.push(FuncType { params, results });
                    }
                }
                3 => {
                    // Function section
                    let count = sec.read_u32_leb()? as usize;
                    for _ in 0..count {
                        func_type_indices.push(sec.read_u32_leb()? as usize);
                    }
                }
                5 => {
                    // Memory section
                    let count = sec.read_u32_leb()? as usize;
                    if count > 0 {
                        let flags = sec.read_u32_leb()?;
                        let initial_pages = sec.read_u32_leb()? as usize;
                        let max_pages = if (flags & 1) != 0 {
                            Some(sec.read_u32_leb()? as usize)
                        } else {
                            None
                        };
                        initial_memory = vec![0u8; initial_pages.min(64) * 65536];
                        max_memory_pages = max_pages;
                    }
                }
                6 => {
                    // Global section
                    let count = sec.read_u32_leb()? as usize;
                    for _ in 0..count {
                        let val_ty = ValType::from_byte(sec.read_u8()?)?;
                        let _mutability = sec.read_u8()?;
                        let init_val = eval_const_expr(&mut sec, val_ty)?;
                        initial_globals.push(init_val);
                    }
                }
                7 => {
                    // Export section
                    let count = sec.read_u32_leb()? as usize;
                    for _ in 0..count {
                        let name = sec.read_string()?;
                        let kind = sec.read_u8()?;
                        let idx = sec.read_u32_leb()? as usize;
                        if kind == 0x00 {
                            exports.push((name, idx));
                        }
                    }
                }
                10 => {
                    // Code section
                    let count = sec.read_u32_leb()? as usize;
                    for _ in 0..count {
                        let body_size = sec.read_u32_leb()? as usize;
                        let body_bytes = sec.read_bytes(body_size)?;
                        let mut bcur = Cursor::new(body_bytes);
                        let local_decl_count = bcur.read_u32_leb()? as usize;
                        let mut locals = Vec::new();
                        for _ in 0..local_decl_count {
                            let repeat = bcur.read_u32_leb()? as usize;
                            let ty = ValType::from_byte(bcur.read_u8()?)?;
                            for _ in 0..repeat {
                                locals.push(ty);
                            }
                        }
                        let code = body_bytes[bcur.pos..].to_vec();
                        bodies.push(FunctionBody { locals, code });
                    }
                }
                11 => {
                    // Data section
                    let count = sec.read_u32_leb()? as usize;
                    for _ in 0..count {
                        let mode = sec.read_u32_leb()?;
                        if mode == 0 {
                            let offset = match eval_const_expr(&mut sec, ValType::I32)? {
                                WasmVal::I32(v) => v as usize,
                                _ => 0,
                            };
                            let data_len = sec.read_u32_leb()? as usize;
                            let data = sec.read_bytes(data_len)?;
                            if offset + data_len > initial_memory.len() {
                                initial_memory.resize(offset + data_len, 0);
                            }
                            initial_memory[offset..offset + data_len].copy_from_slice(data);
                        } else if mode == 1 {
                            let data_len = sec.read_u32_leb()? as usize;
                            let _ = sec.read_bytes(data_len)?;
                        }
                    }
                }
                _ => {
                    // Custom or unneeded section
                }
            }
        }

        if func_type_indices.len() != bodies.len() {
            return Err(Error::InvalidModule(
                "function section count does not match code section count".to_string(),
            ));
        }

        Ok(Self {
            types,
            func_type_indices,
            exports,
            bodies,
            initial_memory,
            max_memory_pages,
            initial_globals,
        })
    }

    fn find_export(&self, name: &str) -> Option<usize> {
        self.exports
            .iter()
            .find(|(export_name, _)| export_name == name)
            .map(|(_, idx)| *idx)
    }

    fn func_signature(&self, func_idx: usize) -> Result<&FuncType, Error> {
        let type_idx = *self
            .func_type_indices
            .get(func_idx)
            .ok_or_else(|| Error::InvalidModule(format!("invalid function index {func_idx}")))?;
        self.types
            .get(type_idx)
            .ok_or_else(|| Error::InvalidModule(format!("invalid type index {type_idx}")))
    }

    pub fn invoke(&self, func_idx: usize, args: &[WasmVal]) -> Result<Option<WasmVal>, Error> {
        let mut memory = self.initial_memory.clone();
        let mut globals = self.initial_globals.clone();
        self.call_func(func_idx, args, &mut memory, &mut globals, 0)
    }

    fn call_func(
        &self,
        func_idx: usize,
        args: &[WasmVal],
        memory: &mut Vec<u8>,
        globals: &mut Vec<WasmVal>,
        depth: usize,
    ) -> Result<Option<WasmVal>, Error> {
        if depth > 64 {
            return Err(Error::Trap("call stack depth exceeded".to_string()));
        }
        let sig = self.func_signature(func_idx)?;
        let body = &self.bodies[func_idx];

        let mut locals = Vec::with_capacity(args.len() + body.locals.len());
        locals.extend_from_slice(args);
        for ty in &body.locals {
            locals.push(WasmVal::default_for(*ty));
        }

        let mut stack: Vec<WasmVal> = Vec::new();
        let mut ctrl_stack: Vec<CtrlFrame> = vec![CtrlFrame {
            kind: CtrlKind::Function,
            start_pc: 0,
                        end_pc: body.code.len().saturating_sub(1),
            stack_height: 0,
            arity: sig.results.len(),
        }];

        let block_map = precompute_blocks(&body.code)?;
        let mut pc = 0usize;

        while pc < body.code.len() {
            let op = body.code[pc];
            pc += 1;

            match op {
                0x00 => return Err(Error::Trap("unreachable executed".to_string())),
                0x01 => {} // nop
                0x02 => {
                    // block
                    let arity = read_block_arity(&body.code, &mut pc)?;
                    let info = block_map.get(&pc).ok_or_else(|| {
                        Error::InvalidModule(format!("missing block metadata at pc {pc}"))
                    })?;
                    ctrl_stack.push(CtrlFrame {
                        kind: CtrlKind::Block,
                        start_pc: pc,
                                                end_pc: info.end_pc,
                        stack_height: stack.len(),
                        arity,
                    });
                }
                0x03 => {
                    // loop
                    let _arity = read_block_arity(&body.code, &mut pc)?;
                    let info = block_map.get(&pc).ok_or_else(|| {
                        Error::InvalidModule(format!("missing loop metadata at pc {pc}"))
                    })?;
                    ctrl_stack.push(CtrlFrame {
                        kind: CtrlKind::Loop,
                        start_pc: pc,
                                                end_pc: info.end_pc,
                        stack_height: stack.len(),
                        arity: 0,
                    });
                }
                0x04 => {
                    // if
                    let arity = read_block_arity(&body.code, &mut pc)?;
                    let info = block_map.get(&pc).ok_or_else(|| {
                        Error::InvalidModule(format!("missing if metadata at pc {pc}"))
                    })?;
                    let cond = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    ctrl_stack.push(CtrlFrame {
                        kind: CtrlKind::If,
                        start_pc: pc,
                                                end_pc: info.end_pc,
                        stack_height: stack.len(),
                        arity,
                    });
                    if cond == 0 {
                        if let Some(else_target) = info.else_pc {
                            pc = else_target;
                        } else {
                            ctrl_stack.pop();
                            pc = info.end_pc;
                        }
                    }
                }
                0x05 => {
                    // else
                    let frame = ctrl_stack
                        .pop()
                        .ok_or_else(|| Error::Trap("control stack underflow on else".into()))?;
                    pc = frame.end_pc;
                }
                0x0B => {
                    // end
                    let frame = ctrl_stack
                        .pop()
                        .ok_or_else(|| Error::Trap("control stack underflow on end".into()))?;
                    if ctrl_stack.is_empty() {
                        break;
                    }
                    let _ = frame;
                }
                0x0C => {
                    // br
                    let label_idx = read_u32_at(&body.code, &mut pc)? as usize;
                    branch_to_label(label_idx, &mut stack, &mut ctrl_stack, &mut pc)?;
                }
                0x0D => {
                    // br_if
                    let label_idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let cond = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    if cond != 0 {
                        branch_to_label(label_idx, &mut stack, &mut ctrl_stack, &mut pc)?;
                    }
                }
                0x0F => {
                    // return
                    break;
                }
                0x10 => {
                    // call
                    let callee_idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let callee_sig = self.func_signature(callee_idx)?;
                    let mut callee_args = vec![WasmVal::I32(0); callee_sig.params.len()];
                    for i in (0..callee_sig.params.len()).rev() {
                        callee_args[i] = stack
                            .pop()
                            .ok_or_else(|| Error::Trap("stack underflow on call".into()))?;
                    }
                    if let Some(ret) =
                        self.call_func(callee_idx, &callee_args, memory, globals, depth + 1)?
                    {
                        stack.push(ret);
                    }
                }
                0x1A => {
                    // drop
                    stack.pop().ok_or_else(|| Error::Trap("stack underflow on drop".into()))?;
                }
                0x1B => {
                    // select
                    let c = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let val2 = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?;
                    let val1 = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?;
                    stack.push(if c != 0 { val1 } else { val2 });
                }
                0x20 => {
                    // local.get
                    let idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = *locals
                        .get(idx)
                        .ok_or_else(|| Error::Trap(format!("invalid local index {idx}")))?;
                    stack.push(val);
                }
                0x21 => {
                    // local.set
                    let idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?;
                    let slot = locals
                        .get_mut(idx)
                        .ok_or_else(|| Error::Trap(format!("invalid local index {idx}")))?;
                    *slot = val;
                }
                0x22 => {
                    // local.tee
                    let idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = *stack.last().ok_or_else(|| Error::Trap("stack underflow".into()))?;
                    let slot = locals
                        .get_mut(idx)
                        .ok_or_else(|| Error::Trap(format!("invalid local index {idx}")))?;
                    *slot = val;
                }
                0x23 => {
                    // global.get
                    let idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = *globals
                        .get(idx)
                        .ok_or_else(|| Error::Trap(format!("invalid global index {idx}")))?;
                    stack.push(val);
                }
                0x24 => {
                    // global.set
                    let idx = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?;
                    let slot = globals
                        .get_mut(idx)
                        .ok_or_else(|| Error::Trap(format!("invalid global index {idx}")))?;
                    *slot = val;
                }
                0x28 => {
                    // i32.load
                    let _align = read_u32_at(&body.code, &mut pc)?;
                    let offset = read_u32_at(&body.code, &mut pc)? as usize;
                    let base = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()? as usize;
                    let addr = base + offset;
                    if addr + 4 > memory.len() {
                        return Err(Error::Trap("out of bounds memory access".into()));
                    }
                    let v = i32::from_le_bytes([
                        memory[addr],
                        memory[addr + 1],
                        memory[addr + 2],
                        memory[addr + 3],
                    ]);
                    stack.push(WasmVal::I32(v));
                }
                0x29 => {
                    // i64.load
                    let _align = read_u32_at(&body.code, &mut pc)?;
                    let offset = read_u32_at(&body.code, &mut pc)? as usize;
                    let base = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()? as usize;
                    let addr = base + offset;
                    if addr + 8 > memory.len() {
                        return Err(Error::Trap("out of bounds memory access".into()));
                    }
                    let v = i64::from_le_bytes([
                        memory[addr],
                        memory[addr + 1],
                        memory[addr + 2],
                        memory[addr + 3],
                        memory[addr + 4],
                        memory[addr + 5],
                        memory[addr + 6],
                        memory[addr + 7],
                    ]);
                    stack.push(WasmVal::I64(v));
                }
                0x36 => {
                    // i32.store
                    let _align = read_u32_at(&body.code, &mut pc)?;
                    let offset = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let base = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()? as usize;
                    let addr = base + offset;
                    if addr + 4 > memory.len() {
                        return Err(Error::Trap("out of bounds memory access".into()));
                    }
                    memory[addr..addr + 4].copy_from_slice(&val.to_le_bytes());
                }
                0x37 => {
                    // i64.store
                    let _align = read_u32_at(&body.code, &mut pc)?;
                    let offset = read_u32_at(&body.code, &mut pc)? as usize;
                    let val = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    let base = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()? as usize;
                    let addr = base + offset;
                    if addr + 8 > memory.len() {
                        return Err(Error::Trap("out of bounds memory access".into()));
                    }
                    memory[addr..addr + 8].copy_from_slice(&val.to_le_bytes());
                }
                0x3F => {
                    // memory.size
                    let _reserved = read_u8_at(&body.code, &mut pc)?;
                    stack.push(WasmVal::I32((memory.len() / 65536) as i32));
                }
                0x40 => {
                    // memory.grow
                    let _reserved = read_u8_at(&body.code, &mut pc)?;
                    let delta = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()? as usize;
                    let cur_pages = memory.len() / 65536;
                    let new_pages = cur_pages + delta;
                    if new_pages > self.max_memory_pages.unwrap_or(64) {
                        stack.push(WasmVal::I32(-1));
                    } else {
                        memory.resize(new_pages * 65536, 0);
                        stack.push(WasmVal::I32(cur_pages as i32));
                    }
                }
                0x41 => {
                    let v = read_i32_at(&body.code, &mut pc)?;
                    stack.push(WasmVal::I32(v));
                }
                0x42 => {
                    let v = read_i64_at(&body.code, &mut pc)?;
                    stack.push(WasmVal::I64(v));
                }
                0x43 => {
                    let v = read_f32_at(&body.code, &mut pc)?;
                    stack.push(WasmVal::F32(v));
                }
                0x44 => {
                    let v = read_f64_at(&body.code, &mut pc)?;
                    stack.push(WasmVal::F64(v));
                }
                // i32 comparisons
                0x45 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::I32(i32::from(a == 0)));
                }
                0x46..=0x4F => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let res = match op {
                        0x46 => a == b,
                        0x47 => a != b,
                        0x48 => a < b,
                        0x49 => (a as u32) < (b as u32),
                        0x4A => a > b,
                        0x4B => (a as u32) > (b as u32),
                        0x4C => a <= b,
                        0x4D => (a as u32) <= (b as u32),
                        0x4E => a >= b,
                        0x4F => (a as u32) >= (b as u32),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I32(i32::from(res)));
                }
                // i64 comparisons
                0x50 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::I32(i32::from(a == 0)));
                }
                0x51..=0x5A => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    let res = match op {
                        0x51 => a == b,
                        0x52 => a != b,
                        0x53 => a < b,
                        0x54 => (a as u64) < (b as u64),
                        0x55 => a > b,
                        0x56 => (a as u64) > (b as u64),
                        0x57 => a <= b,
                        0x58 => (a as u64) <= (b as u64),
                        0x59 => a >= b,
                        0x5A => (a as u64) >= (b as u64),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I32(i32::from(res)));
                }
                // f32 comparisons
                0x5B..=0x60 => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    let res = match op {
                        0x5B => a == b,
                        0x5C => a != b,
                        0x5D => a < b,
                        0x5E => a > b,
                        0x5F => a <= b,
                        0x60 => a >= b,
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I32(i32::from(res)));
                }
                // f64 comparisons
                0x61..=0x66 => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    let res = match op {
                        0x61 => a == b,
                        0x62 => a != b,
                        0x63 => a < b,
                        0x64 => a > b,
                        0x65 => a <= b,
                        0x66 => a >= b,
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I32(i32::from(res)));
                }
                // i32 unary & binary ops
                0x67..=0x69 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let res = match op {
                        0x67 => a.leading_zeros() as i32,
                        0x68 => a.trailing_zeros() as i32,
                        0x69 => a.count_ones() as i32,
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I32(res));
                }
                0x6A..=0x78 => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    let res = match op {
                        0x6A => a.wrapping_add(b),
                        0x6B => a.wrapping_sub(b),
                        0x6C => a.wrapping_mul(b),
                        0x6D => {
                            if b == 0 {
                                return Err(Error::Trap("integer divide by zero".into()));
                            }
                            a.wrapping_div(b)
                        }
                        0x6E => {
                            if b == 0 {
                                return Err(Error::Trap("integer divide by zero".into()));
                            }
                            ((a as u32) / (b as u32)) as i32
                        }
                        0x6F => {
                            if b == 0 {
                                return Err(Error::Trap("integer remainder by zero".into()));
                            }
                            a.wrapping_rem(b)
                        }
                        0x70 => {
                            if b == 0 {
                                return Err(Error::Trap("integer remainder by zero".into()));
                            }
                            ((a as u32) % (b as u32)) as i32
                        }
                        0x71 => a & b,
                        0x72 => a | b,
                        0x73 => a ^ b,
                        0x74 => a.wrapping_shl(b as u32),
                        0x75 => a.wrapping_shr(b as u32),
                        0x76 => ((a as u32).wrapping_shr(b as u32)) as i32,
                        0x77 => a.rotate_left(b as u32),
                        0x78 => a.rotate_right(b as u32),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I32(res));
                }
                // i64 unary & binary ops
                0x79..=0x7B => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    let res = match op {
                        0x79 => i64::from(a.leading_zeros()),
                        0x7A => i64::from(a.trailing_zeros()),
                        0x7B => i64::from(a.count_ones()),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I64(res));
                }
                0x7C..=0x8A => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    let res = match op {
                        0x7C => a.wrapping_add(b),
                        0x7D => a.wrapping_sub(b),
                        0x7E => a.wrapping_mul(b),
                        0x7F => {
                            if b == 0 {
                                return Err(Error::Trap("integer divide by zero".into()));
                            }
                            a.wrapping_div(b)
                        }
                        0x80 => {
                            if b == 0 {
                                return Err(Error::Trap("integer divide by zero".into()));
                            }
                            ((a as u64) / (b as u64)) as i64
                        }
                        0x81 => {
                            if b == 0 {
                                return Err(Error::Trap("integer remainder by zero".into()));
                            }
                            a.wrapping_rem(b)
                        }
                        0x82 => {
                            if b == 0 {
                                return Err(Error::Trap("integer remainder by zero".into()));
                            }
                            ((a as u64) % (b as u64)) as i64
                        }
                        0x83 => a & b,
                        0x84 => a | b,
                        0x85 => a ^ b,
                        0x86 => a.wrapping_shl(b as u32),
                        0x87 => a.wrapping_shr(b as u32),
                        0x88 => ((a as u64).wrapping_shr(b as u32)) as i64,
                        0x89 => a.rotate_left(b as u32),
                        0x8A => a.rotate_right(b as u32),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::I64(res));
                }
                // f32 unary & binary ops
                0x8B..=0x91 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    let res = match op {
                        0x8B => a.abs(),
                        0x8C => -a,
                        0x8D => a.ceil(),
                        0x8E => a.floor(),
                        0x8F => a.trunc(),
                        0x90 => a.round(),
                        0x91 => a.sqrt(),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::F32(res));
                }
                0x92..=0x98 => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    let res = match op {
                        0x92 => a + b,
                        0x93 => a - b,
                        0x94 => a * b,
                        0x95 => a / b,
                        0x96 => a.min(b),
                        0x97 => a.max(b),
                        0x98 => a.copysign(b),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::F32(res));
                }
                // f64 unary & binary ops
                0x99..=0x9F => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    let res = match op {
                        0x99 => a.abs(),
                        0x9A => -a,
                        0x9B => a.ceil(),
                        0x9C => a.floor(),
                        0x9D => a.trunc(),
                        0x9E => a.round(),
                        0x9F => a.sqrt(),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::F64(res));
                }
                0xA0..=0xA6 => {
                    let b = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    let res = match op {
                        0xA0 => a + b,
                        0xA1 => a - b,
                        0xA2 => a * b,
                        0xA3 => a / b,
                        0xA4 => a.min(b),
                        0xA5 => a.max(b),
                        0xA6 => a.copysign(b),
                        _ => unreachable!(),
                    };
                    stack.push(WasmVal::F64(res));
                }
                // Conversions
                0xA7 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::I32(a as i32));
                }
                0xA8 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    stack.push(WasmVal::I32(a as i32));
                }
                0xA9 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    stack.push(WasmVal::I32((a as u32) as i32));
                }
                0xAA => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    stack.push(WasmVal::I32(a as i32));
                }
                0xAB => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    stack.push(WasmVal::I32((a as u32) as i32));
                }
                0xAC => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::I64(i64::from(a)));
                }
                0xAD => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::I64(i64::from(a as u32)));
                }
                0xAE => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    stack.push(WasmVal::I64(a as i64));
                }
                0xAF => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    stack.push(WasmVal::I64((a as u64) as i64));
                }
                0xB0 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    stack.push(WasmVal::I64(a as i64));
                }
                0xB1 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    stack.push(WasmVal::I64((a as u64) as i64));
                }
                0xB2 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::F32(a as f32));
                }
                0xB3 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::F32((a as u32) as f32));
                }
                0xB4 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::F32(a as f32));
                }
                0xB5 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::F32((a as u64) as f32));
                }
                0xB6 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    stack.push(WasmVal::F32(a as f32));
                }
                0xB7 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::F64(f64::from(a)));
                }
                0xB8 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::F64(f64::from(a as u32)));
                }
                0xB9 => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::F64(a as f64));
                }
                0xBA => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::F64((a as u64) as f64));
                }
                0xBB => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    stack.push(WasmVal::F64(f64::from(a)));
                }
                0xBC => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f32()?;
                    stack.push(WasmVal::I32(a.to_bits() as i32));
                }
                0xBD => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_f64()?;
                    stack.push(WasmVal::I64(a.to_bits() as i64));
                }
                0xBE => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::F32(f32::from_bits(a as u32)));
                }
                0xBF => {
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i64()?;
                    stack.push(WasmVal::F64(f64::from_bits(a as u64)));
                }
                0xC0 => {
                    // i32.extend8_s
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::I32(i32::from(a as i8)));
                }
                0xC1 => {
                    // i32.extend16_s
                    let a = stack.pop().ok_or_else(|| Error::Trap("stack underflow".into()))?.as_i32()?;
                    stack.push(WasmVal::I32(i32::from(a as i16)));
                }
                other => {
                    return Err(Error::Trap(format!("unsupported opcode 0x{other:02x}")));
                }
            }
        }

        if sig.results.is_empty() {
            Ok(None)
        } else {
            stack
                .pop()
                .map(Some)
                .ok_or_else(|| Error::Trap("missing return value on stack".to_string()))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CtrlKind {
    Function,
    Block,
    Loop,
    If,
}

#[derive(Debug, Clone)]
struct CtrlFrame {
    kind: CtrlKind,
    start_pc: usize,
    end_pc: usize,
    stack_height: usize,
    arity: usize,
}

#[derive(Debug, Clone)]
struct BlockInfo {
    else_pc: Option<usize>,
    end_pc: usize,
}

fn eval_const_expr(sec: &mut Cursor<'_>, expected: ValType) -> Result<WasmVal, Error> {
    let mut val = WasmVal::default_for(expected);
    loop {
        let op = sec.read_u8()?;
        match op {
            0x0B => break,
            0x41 => val = WasmVal::I32(sec.read_i32_leb()?),
            0x42 => val = WasmVal::I64(sec.read_i64_leb()?),
            0x43 => val = WasmVal::F32(sec.read_f32()?),
            0x44 => val = WasmVal::F64(sec.read_f64()?),
            other => {
                return Err(Error::InvalidModule(format!(
                    "unsupported constant expression opcode 0x{other:02x}"
                )))
            }
        }
    }
    Ok(val)
}

fn read_u8_at(code: &[u8], pc: &mut usize) -> Result<u8, Error> {
    if *pc >= code.len() {
        return Err(Error::InvalidModule("unexpected EOF in code".into()));
    }
    let b = code[*pc];
    *pc += 1;
    Ok(b)
}

fn read_u32_at(code: &[u8], pc: &mut usize) -> Result<u32, Error> {
    let mut cur = Cursor {
        bytes: code,
        pos: *pc,
    };
    let v = cur.read_u32_leb()?;
    *pc = cur.pos;
    Ok(v)
}

fn read_i32_at(code: &[u8], pc: &mut usize) -> Result<i32, Error> {
    let mut cur = Cursor {
        bytes: code,
        pos: *pc,
    };
    let v = cur.read_i32_leb()?;
    *pc = cur.pos;
    Ok(v)
}

fn read_i64_at(code: &[u8], pc: &mut usize) -> Result<i64, Error> {
    let mut cur = Cursor {
        bytes: code,
        pos: *pc,
    };
    let v = cur.read_i64_leb()?;
    *pc = cur.pos;
    Ok(v)
}

fn read_f32_at(code: &[u8], pc: &mut usize) -> Result<f32, Error> {
    let mut cur = Cursor {
        bytes: code,
        pos: *pc,
    };
    let v = cur.read_f32()?;
    *pc = cur.pos;
    Ok(v)
}

fn read_f64_at(code: &[u8], pc: &mut usize) -> Result<f64, Error> {
    let mut cur = Cursor {
        bytes: code,
        pos: *pc,
    };
    let v = cur.read_f64()?;
    *pc = cur.pos;
    Ok(v)
}

fn read_block_arity(code: &[u8], pc: &mut usize) -> Result<usize, Error> {
    let b = read_u8_at(code, pc)?;
    if b == 0x40 {
        Ok(0)
    } else {
        let _ = ValType::from_byte(b)?;
        Ok(1)
    }
}

fn branch_to_label(
    label_idx: usize,
    stack: &mut Vec<WasmVal>,
    ctrl_stack: &mut Vec<CtrlFrame>,
    pc: &mut usize,
) -> Result<(), Error> {
    if label_idx >= ctrl_stack.len() {
        return Err(Error::Trap("invalid branch label index".into()));
    }
    let target_pos = ctrl_stack.len() - 1 - label_idx;
    let target = ctrl_stack[target_pos].clone();

    let kept = if target.arity > 0 {
        Some(
            stack
                .pop()
                .ok_or_else(|| Error::Trap("stack underflow on branch".into()))?,
        )
    } else {
        None
    };

    stack.truncate(target.stack_height);
    if let Some(v) = kept {
        stack.push(v);
    }

    if target.kind == CtrlKind::Loop {
        ctrl_stack.truncate(target_pos + 1);
        *pc = target.start_pc;
    } else {
        ctrl_stack.truncate(target_pos);
        *pc = target.end_pc;
    }
    Ok(())
}

fn precompute_blocks(code: &[u8]) -> Result<std::collections::HashMap<usize, BlockInfo>, Error> {
    let mut map = std::collections::HashMap::new();
    let mut open_stack: Vec<(usize, Option<usize>)> = Vec::new();
    let mut pc = 0usize;

    while pc < code.len() {
        let op = code[pc];
        pc += 1;
        match op {
            0x02..=0x04 => {
                let _ = read_block_arity(code, &mut pc)?;
                open_stack.push((pc, None));
            }
            0x05 => {
                if let Some((_, else_slot)) = open_stack.last_mut() {
                    *else_slot = Some(pc);
                }
            }
            0x0B => {
                if let Some((start_pc, else_pc)) = open_stack.pop() {
                    map.insert(start_pc, BlockInfo { else_pc, end_pc: pc });
                }
            }
            0x0C | 0x0D | 0x10 | 0x20..=0x24 => {
                let _ = read_u32_at(code, &mut pc)?;
            }
            0x28..=0x3E => {
                let _ = read_u32_at(code, &mut pc)?;
                let _ = read_u32_at(code, &mut pc)?;
            }
            0x3F | 0x40 => {
                let _ = read_u8_at(code, &mut pc)?;
            }
            0x41 => {
                let _ = read_i32_at(code, &mut pc)?;
            }
            0x42 => {
                let _ = read_i64_at(code, &mut pc)?;
            }
            0x43 => {
                let _ = read_f32_at(code, &mut pc)?;
            }
            0x44 => {
                let _ = read_f64_at(code, &mut pc)?;
            }
            _ => {}
        }
    }

    Ok(map)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::ExpressionBuilder;
    use dozer_types::{
        models::udf_config::{UdfConfig, UdfType, WasmConfig},
        ordered_float::OrderedFloat,
        types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
    };
    use sqlparser::{dialect::GenericDialect, parser::Parser};
    use std::io::Write;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    // WebAssembly binary for:
    // (module
    //   (func (export "my_function") (param i64 i64) (result i64)
    //     local.get 0
    //     local.get 1
    //     i64.add))
    const WASM_ADD_I64: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, // magic: \0asm
        0x01, 0x00, 0x00, 0x00, // version: 1
        // Type section: 1 func type (i64, i64) -> i64
        0x01, 0x07, 0x01, 0x60, 0x02, 0x7e, 0x7e, 0x01, 0x7e,
        // Function section: 1 function of type 0
        0x03, 0x02, 0x01, 0x00,
        // Export section: "my_function" -> func 0
        0x07, 0x0f, 0x01, 0x0b, b'm', b'y', b'_', b'f', b'u', b'n', b'c', b't', b'i', b'o', b'n', 0x00, 0x00,
        // Code section: body(local.get 0, local.get 1, i64.add, end)
        0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x7c, 0x0b,
    ];

    // WebAssembly binary for:
    // (module
    //   (func (export "weighted_sum") (param f64 f64) (result f64)
    //     local.get 0
    //     local.get 1
    //     f64.mul
    //     local.get 0
    //     f64.add))
    const WASM_WEIGHTED_F64: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d,
        0x01, 0x00, 0x00, 0x00,
        0x01, 0x07, 0x01, 0x60, 0x02, 0x7c, 0x7c, 0x01, 0x7c,
        0x03, 0x02, 0x01, 0x00,
        0x07, 0x10, 0x01, 0x0c, b'w', b'e', b'i', b'g', b'h', b't', b'e', b'd', b'_', b's', b'u', b'm', 0x00, 0x00,
        0x0a, 0x0c, 0x01, 0x0a, 0x00, 0x20, 0x00, 0x20, 0x01, 0xa2, 0x20, 0x00, 0xa0, 0x0b,
    ];

    #[test]
    fn test_wasm_udf_sql_evaluation_and_extensionless_path() {
        let dir = std::env::temp_dir().join(format!("dozer_wasm_test_{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let wasm_file_path = dir.join("my_function.wasm");
        {
            let mut file = fs::File::create(&wasm_file_path).unwrap();
            file.write_all(WASM_ADD_I64).unwrap();
        }

        // Pass path WITHOUT .wasm extension as specified in issue #1653: udfs/wasm/my_function
        let extensionless_path = dir.join("my_function").to_string_lossy().to_string();
        let udfs = vec![UdfConfig {
            name: "my_function".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: extensionless_path,
                function: None,
            }),
        }];

        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "field1".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "field2".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone();

        let sql = "SELECT my_function(field1, field2) FROM table_1";
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).unwrap();
        let projection = match &ast[0] {
            sqlparser::ast::Statement::Query(q) => match &*q.body {
                sqlparser::ast::SetExpr::Select(s) => &s.projection[0],
                _ => panic!("expected select"),
            },
            _ => panic!("expected query"),
        };
        let expr_ast = match projection {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
            _ => panic!("expected unnamed expr"),
        };

        let runtime = Arc::new(Runtime::new().unwrap());
        let mut builder = ExpressionBuilder::new(0, runtime.clone());
        let mut expr = runtime
            .block_on(builder.parse_sql_expression(false, expr_ast, &schema, &udfs))
            .unwrap();

        let expr_type = expr.get_type(&schema).unwrap();
        assert_eq!(expr_type.return_type, FieldType::Int);
        assert_eq!(expr.to_string(&schema), "my_function(field1,field2)");

        let record = Record::new(vec![Field::Int(19), Field::Int(23)]);
        let result = expr.evaluate(&record, &schema).unwrap();
        assert_eq!(result, Field::Int(42));

        let null_record = Record::new(vec![Field::Int(19), Field::Null]);
        let null_result = expr.evaluate(&null_record, &schema).unwrap();
        assert_eq!(null_result, Field::Null);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wasm_udf_float_and_int_coercion() {
        let dir = std::env::temp_dir().join(format!("dozer_wasm_f64_test_{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let wasm_file_path = dir.join("weighted_sum.wasm");
        {
            let mut file = fs::File::create(&wasm_file_path).unwrap();
            file.write_all(WASM_WEIGHTED_F64).unwrap();
        }

        let udfs = vec![UdfConfig {
            name: "weighted_sum".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: wasm_file_path.to_string_lossy().to_string(),
                function: None,
            }),
        }];

        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "a".to_string(),
                    FieldType::Float,
                    false,
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

        let runtime = Arc::new(Runtime::new().unwrap());
        let mut builder = ExpressionBuilder::new(0, runtime.clone());
        let dialect = GenericDialect {};
        let expr_ast = Parser::new(&dialect)
            .try_with_sql("weighted_sum(a, b)")
            .unwrap()
            .parse_expr()
            .unwrap();

        let mut expr = runtime
            .block_on(builder.parse_sql_expression(false, &expr_ast, &schema, &udfs))
            .unwrap();

        assert_eq!(expr.get_type(&schema).unwrap().return_type, FieldType::Float);
        let record = Record::new(vec![Field::Float(OrderedFloat(2.5)), Field::Int(3)]);
        let result = expr.evaluate(&record, &schema).unwrap();
        assert_eq!(result, Field::Float(OrderedFloat(10.0)));
        let _ = fs::remove_dir_all(&dir);
    }
}
