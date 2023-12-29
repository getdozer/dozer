use odbc::{odbc_safe::AutocommitMode, DiagnosticRecord, Statement};

#[derive(Debug, Clone)]
pub enum OdbcValue {
    Binary(Vec<u8>),
    String(String),
    Decimal(String),
    Timestamp(String),
    Date(String),
    U8(u8),
    I8(i8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Null,
}

impl OdbcValue {
    pub fn bind<'a, 'b, 'c, S, R, AC: AutocommitMode>(
        &'c self,
        statement: Statement<'a, 'b, S, R, AC>,
        parameter_index: u16,
    ) -> Result<Statement<'a, 'c, S, R, AC>, Box<DiagnosticRecord>>
    where
        'b: 'c,
    {
        let statement = match self {
            Self::Binary(value) => statement.bind_parameter(parameter_index, value)?,
            Self::String(value) => statement.bind_parameter(parameter_index, value)?,
            Self::Decimal(value) => statement.bind_parameter(parameter_index, value)?,
            Self::Timestamp(value) => statement.bind_parameter(parameter_index, value)?,
            Self::Date(value) => statement.bind_parameter(parameter_index, value)?,
            Self::U8(value) => statement.bind_parameter(parameter_index, value)?,
            Self::I8(value) => statement.bind_parameter(parameter_index, value)?,
            Self::I16(value) => statement.bind_parameter(parameter_index, value)?,
            Self::U16(value) => statement.bind_parameter(parameter_index, value)?,
            Self::I32(value) => statement.bind_parameter(parameter_index, value)?,
            Self::U32(value) => statement.bind_parameter(parameter_index, value)?,
            Self::I64(value) => statement.bind_parameter(parameter_index, value)?,
            Self::U64(value) => statement.bind_parameter(parameter_index, value)?,
            Self::F32(value) => statement.bind_parameter(parameter_index, value)?,
            Self::F64(value) => statement.bind_parameter(parameter_index, value)?,
            Self::Bool(value) => statement.bind_parameter(parameter_index, value)?,
            Self::Null => statement.bind_parameter(parameter_index, &None::<bool>)?,
        };
        Ok(statement)
    }

    pub fn as_sql(&self) -> String {
        match self {
            Self::Binary(b) => format!("TO_BINARY('{}', 'BASE64')", base64_encode(b)),
            Self::String(s) => {
                format!(
                    "TO_VARCHAR(TO_BINARY('{}', 'BASE64'), 'UTF-8')",
                    base64_encode(s.as_bytes())
                )
            }
            Self::Decimal(s) => s.clone(),
            Self::Timestamp(s) => format!("'{}'", s),
            Self::Date(s) => format!("'{}'", s),
            Self::U8(u) => u.to_string(),
            Self::I8(i) => i.to_string(),
            Self::I16(i) => i.to_string(),
            Self::U16(u) => u.to_string(),
            Self::I32(i) => i.to_string(),
            Self::U32(u) => u.to_string(),
            Self::I64(i) => i.to_string(),
            Self::U64(u) => u.to_string(),
            Self::F32(f) => f.to_string(),
            Self::F64(f) => f.to_string(),
            Self::Bool(b) => b.to_string(),
            Self::Null => "null".to_string(),
        }
    }
}

impl From<Vec<u8>> for OdbcValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(value)
    }
}

impl From<String> for OdbcValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<u8> for OdbcValue {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<i8> for OdbcValue {
    fn from(value: i8) -> Self {
        Self::I8(value)
    }
}

impl From<i16> for OdbcValue {
    fn from(value: i16) -> Self {
        Self::I16(value)
    }
}

impl From<u16> for OdbcValue {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}

impl From<i32> for OdbcValue {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<u32> for OdbcValue {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}

impl From<i64> for OdbcValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u64> for OdbcValue {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<f32> for OdbcValue {
    fn from(value: f32) -> Self {
        Self::F32(value)
    }
}

impl From<f64> for OdbcValue {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<bool> for OdbcValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
