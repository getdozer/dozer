use dozer_types::thiserror;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid wasm module: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValType {
    I32,
    I64,
    F32,
    F64,
    Unsupported(u8),
}

impl ValType {
    fn from_byte(byte: u8) -> Self {
        match byte {
            0x7F => ValType::I32,
            0x7E => ValType::I64,
            0x7D => ValType::F32,
            0x7C => ValType::F64,
            other => ValType::Unsupported(other),
        }
    }

    pub fn is_supported(&self) -> bool {
        !matches!(self, ValType::Unsupported(_))
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, ValType::I32 | ValType::I64)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncSignature {
    pub params: Vec<ValType>,
    pub results: Vec<ValType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedFunction {
    pub module: String,
    pub field: String,
    pub type_idx: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedFunction {
    pub name: String,
    pub func_idx: u32,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ModuleInfo {
    pub types: Vec<FuncSignature>,
    pub imported_functions: Vec<ImportedFunction>,
    pub function_types: Vec<u32>,
    pub exported_functions: Vec<ExportedFunction>,
}

impl ModuleInfo {
    pub fn exported_function_signature(&self, name: &str) -> Result<FuncSignature, Error> {
        let export = self
            .exported_functions
            .iter()
            .find(|export| export.name == name)
            .ok_or_else(|| Error::Invalid(format!("function {name} is not exported")))?;
        let func_idx = export.func_idx as usize;
        let type_idx = if func_idx < self.imported_functions.len() {
            self.imported_functions[func_idx].type_idx as usize
        } else {
            let defined_idx = func_idx - self.imported_functions.len();
            *self
                .function_types
                .get(defined_idx)
                .ok_or_else(|| Error::Invalid(format!("function index {func_idx} is not defined")))?
                as usize
        };
        let signature = self
            .types
            .get(type_idx)
            .ok_or_else(|| Error::Invalid(format!("type index {type_idx} is not defined")))?
            .clone();
        for value_type in signature.params.iter().chain(signature.results.iter()) {
            if !value_type.is_supported() {
                return Err(Error::Invalid(format!(
                    "function {name} has unsupported parameter or result types"
                )));
            }
        }
        Ok(signature)
    }
}

#[derive(Debug)]
pub struct ParsedModule {
    pub bytes: Vec<u8>,
    pub info: ModuleInfo,
}

pub fn parse_module_bytes(bytes: &[u8]) -> Result<ModuleInfo, Error> {
    if bytes.len() < 8 || &bytes[0..4] != b"\0asm" {
        return Err(Error::Invalid("missing wasm magic number".to_string()));
    }
    if &bytes[4..8] != [1, 0, 0, 0] {
        return Err(Error::Invalid("unsupported wasm version".to_string()));
    }

    let mut reader = Reader { data: bytes, pos: 8 };
    let mut info = ModuleInfo::default();
    while reader.pos < reader.data.len() {
        let section_id = reader.byte()?;
        let section_size = reader.u32()? as usize;
        let section_end = reader
            .pos
            .checked_add(section_size)
            .filter(|end| *end <= reader.data.len())
            .ok_or_else(|| Error::Invalid("section size out of bounds".to_string()))?;
        let mut section = Reader {
            data: &reader.data[..section_end],
            pos: reader.pos,
        };
        match section_id {
            1 => parse_type_section(&mut section, &mut info)?,
            2 => parse_import_section(&mut section, &mut info)?,
            3 => parse_function_section(&mut section, &mut info)?,
            7 => parse_export_section(&mut section, &mut info)?,
            _ => {}
        }
        reader.pos = section_end;
    }
    Ok(info)
}

fn parse_type_section(section: &mut Reader, info: &mut ModuleInfo) -> Result<(), Error> {
    let count = section.u32()?;
    for _ in 0..count {
        let tag = section.byte()?;
        if tag != 0x60 {
            return Err(Error::Invalid(format!("unsupported type form: 0x{tag:02x}")));
        }
        let params = parse_value_types(section)?;
        let results = parse_value_types(section)?;
        info.types.push(FuncSignature { params, results });
    }
    Ok(())
}

fn parse_value_types(section: &mut Reader) -> Result<Vec<ValType>, Error> {
    let count = section.u32()?;
    let mut value_types = Vec::with_capacity(count as usize);
    for _ in 0..count {
        value_types.push(ValType::from_byte(section.byte()?));
    }
    Ok(value_types)
}

fn parse_import_section(section: &mut Reader, info: &mut ModuleInfo) -> Result<(), Error> {
    let count = section.u32()?;
    for _ in 0..count {
        let module = section.name()?;
        let field = section.name()?;
        match section.byte()? {
            0x00 => {
                let type_idx = section.u32()?;
                info.imported_functions.push(ImportedFunction {
                    module,
                    field,
                    type_idx,
                });
            }
            0x01 => {
                section.byte()?;
                section.skip_limits()?;
            }
            0x02 => section.skip_limits()?,
            0x03 => {
                section.byte()?;
                section.byte()?;
            }
            kind => {
                return Err(Error::Invalid(format!(
                    "unsupported import kind for {module}.{field}: 0x{kind:02x}"
                )))
            }
        }
    }
    Ok(())
}

fn parse_function_section(section: &mut Reader, info: &mut ModuleInfo) -> Result<(), Error> {
    let count = section.u32()?;
    for _ in 0..count {
        info.function_types.push(section.u32()?);
    }
    Ok(())
}

fn parse_export_section(section: &mut Reader, info: &mut ModuleInfo) -> Result<(), Error> {
    let count = section.u32()?;
    for _ in 0..count {
        let name = section.name()?;
        let kind = section.byte()?;
        let index = section.u32()?;
        if kind == 0x00 {
            info.exported_functions.push(ExportedFunction { name, func_idx: index });
        }
    }
    Ok(())
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, Error> {
        let byte = *self
            .data
            .get(self.pos)
            .ok_or_else(|| Error::Invalid("unexpected end of module".to_string()))?;
        self.pos += 1;
        Ok(byte)
    }

    fn u32(&mut self) -> Result<u32, Error> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = self.byte()? as u64;
            result |= (byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 35 {
                return Err(Error::Invalid("invalid LEB128 number".to_string()));
            }
        }
        u32::try_from(result).map_err(|_| Error::Invalid("LEB128 number out of range".to_string()))
    }

    fn name(&mut self) -> Result<String, Error> {
        let length = self.u32()? as usize;
        let end = self
            .pos
            .checked_add(length)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| Error::Invalid("name out of bounds".to_string()))?;
        let name = String::from_utf8_lossy(&self.data[self.pos..end]).into_owned();
        self.pos = end;
        Ok(name)
    }

    fn skip_limits(&mut self) -> Result<(), Error> {
        let flags = self.byte()?;
        self.u32()?;
        if flags & 0x01 != 0 {
            self.u32()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn add_f64_module() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x60, 0x02, 0x7c,
            0x7c, 0x01, 0x7c, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64,
            0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0xa0, 0x0b,
        ]
    }

    #[test]
    fn parses_exported_function_signature() {
        let info = parse_module_bytes(&add_f64_module()).unwrap();
        let signature = info.exported_function_signature("add").unwrap();
        assert_eq!(
            signature,
            FuncSignature {
                params: vec![ValType::F64, ValType::F64],
                results: vec![ValType::F64],
            }
        );
    }

    #[test]
    fn missing_export_is_an_error() {
        let info = parse_module_bytes(&add_f64_module()).unwrap();
        assert_eq!(
            info.exported_function_signature("sub"),
            Err(Error::Invalid("function sub is not exported".to_string()))
        );
    }

    #[test]
    fn invalid_magic_is_an_error() {
        assert_eq!(
            parse_module_bytes(&[0x00, 0x61, 0x73, 0x00]),
            Err(Error::Invalid("missing wasm magic number".to_string()))
        );
    }
}
