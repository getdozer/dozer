pub fn escape_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

pub fn qualify_table_name(schema: Option<&str>, name: &str) -> String {
    if let Some(schema) = schema {
        format!("{}.{}", escape_identifier(schema), escape_identifier(name))
    } else {
        escape_identifier(name)
    }
}
