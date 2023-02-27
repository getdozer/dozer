use super::{
    helper::{self, get_sample_ops},
    TestInstruction,
};

// Testing CTEs, nested queries, aliases and Joins.
#[test]
fn cte_query() {
    // CTE Test

    let queries = vec![
        r#"
            WITH tbl as (
                SELECT actor_id, first_name, last_name from actor
            ) 
            SELECT actor_id, first_name, last_name from tbl;
        "#,
    ];

    let list = get_sample_ops();

    helper::compare_with_sqlite(&["actor"], &queries, TestInstruction::List(list));
}

#[test]
fn nested_query() {
    // CTE Test
    let queries = vec![
        r#" 
            SELECT actor_id, first_name, last_name from (
                SELECT actor_id, first_name, last_name from actor
            );
        "#,
    ];

    helper::compare_with_sqlite(
        &["actor"],
        &queries,
        TestInstruction::FromCsv("actor", vec!["actor"]),
    );

    helper::compare_with_sqlite(
        &["actor"],
        &queries,
        TestInstruction::List(get_sample_ops()),
    );
}

#[test]
fn nested_agg_inserts_query() {
    let query = r#" 
            SELECT actor_id, count(actor_id) from (
                SELECT actor_id, first_name, last_name from actor
            ) a
            GROUP By actor_id;
        "#;

    // Insert Only Operation
    let result = helper::query(
        &["actor"],
        query,
        &None,
        &TestInstruction::FromCsv("actor", vec!["actor"]),
    );

    assert_eq!(
        result.source_result.len(),
        result.dest_result.len(),
        "must be equal"
    );
}

#[test]
fn nested_agg_updates_query() {
    let query = r#" 
            SELECT actor_id, count(actor_id) from (
                SELECT actor_id, first_name, last_name from actor
            ) a
            GROUP By actor_id;
        "#;

    // Insert, Delete and Update

    let result = helper::query(
        &["actor"],
        query,
        &None,
        &TestInstruction::List(helper::get_sample_ops()),
    );

    assert_eq!(
        result.source_result.len(),
        result.dest_result.len(),
        "must be equal"
    );
}

#[test]
fn cte_agg_inserts_query() {
    let query = r#"
            WITH tbl as (
                SELECT actor_id, first_name, last_name from actor
            )
            SELECT actor_id, count(actor_id) from tbl
            GROUP By actor_id;
        "#;

    // // Insert Only Operation
    let result = helper::query(
        &["actor"],
        query,
        &None,
        &TestInstruction::FromCsv("actor", vec!["actor"]),
    );

    assert_eq!(
        result.source_result.len(),
        result.dest_result.len(),
        "must be equal"
    );
}

#[test]
fn cte_agg_updates_query() {
    let query = r#"
            WITH tbl as (
                SELECT actor_id, first_name, last_name from actor
            )
            SELECT actor_id, count(actor_id) from tbl
            GROUP By actor_id;
        "#;

    // Insert, Delete and Update

    let result = helper::query(
        &["actor"],
        query,
        &None,
        &TestInstruction::List(helper::get_sample_ops()),
    );

    assert_eq!(
        result.source_result.len(),
        result.dest_result.len(),
        "must be equal"
    );
}
