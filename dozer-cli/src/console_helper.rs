pub fn get_colored_text(value: &str, code: &str) -> String {
    let mut result = String::from("\u{1b}[");
    result.push_str(code);
    result.push_str(";1m"); // End token.
    result.push_str(value);
    result.push_str("\u{1b}[0m");
    result
}
