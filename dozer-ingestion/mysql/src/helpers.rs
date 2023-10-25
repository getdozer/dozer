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

#[cfg(test)]
mod tests {
    use crate::helpers::{escape_identifier, qualify_table_name};

    #[test]
    fn test_identifiers() {
        assert_eq!(escape_identifier("test"), String::from("`test`"));
        assert_eq!(
            qualify_table_name(Some("db"), "test"),
            String::from("`db`.`test`")
        );
        assert_eq!(qualify_table_name(None, "test"), String::from("`test`"));
    }
}
