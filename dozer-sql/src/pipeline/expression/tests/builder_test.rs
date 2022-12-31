use crate::pipeline::expression::builder::compare_name;

#[test]
fn test_compare_name() {
    assert!(compare_name("name".to_string(), "name".to_string()));
    assert!(compare_name("table.name".to_string(), "name".to_string()));
    assert!(compare_name("name".to_string(), "table.name".to_string()));
    assert!(compare_name(
        "table_a.name".to_string(),
        "table_a.name".to_string()
    ));

    assert!(!compare_name(
        "table_a.name".to_string(),
        "table_b.name".to_string()
    ));

    assert!(compare_name(
        "conn.table_a.name".to_string(),
        "name".to_string()
    ));

    assert!(compare_name(
        "conn.table_a.name".to_string(),
        "table_a.name".to_string()
    ));

    assert!(!compare_name(
        "conn.table_a.name".to_string(),
        "table_b.name".to_string()
    ));

    assert!(!compare_name(
        "conn.table_a.name".to_string(),
        "conn.table_b.name".to_string()
    ));

    assert!(compare_name(
        "conn.table_a.name".to_string(),
        "conn.table_a.name".to_string()
    ));

    assert!(!compare_name(
        "conn_a.table_a.name".to_string(),
        "conn_b.table_a.name".to_string()
    ));
}

#[test]
fn test_get_field_index() {
    todo!()
}
