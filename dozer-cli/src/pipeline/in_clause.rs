use std::collections::HashMap;

/// Evaluates an `IN` clause against a record.
///
/// * `record` – a map from column name to its string representation.
/// * `column` – the column name to be checked.
/// * `values` – the list of allowed values (as strings).
///
/// Returns `true` if the value of `column` in `record` is present in `values`.
pub fn evaluate_in(
    record: &HashMap<String, String>,
    column: &str,
    values: &[String],
) -> bool {
    match record.get(column) {
        Some(v) => values.iter().any(|val| val == v),
        None => false,
    }
}
