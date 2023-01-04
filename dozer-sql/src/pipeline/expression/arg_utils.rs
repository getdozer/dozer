#[macro_export]
macro_rules! arg_str {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_string() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_uint {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_uint() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_int {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_int() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_float {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_float() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_binary {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_binary() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_decimal {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_decimal() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_timestamp {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_timestamp() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_date {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_date() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}
