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

    helper::compare_with_sqlite(&vec!["actor"], queries, TestInstruction::List(list));
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

    let list = get_sample_ops();

    helper::compare_with_sqlite(&vec!["actor"], queries, TestInstruction::List(list));
}

#[ignore]
#[test]
fn nested_agg_query() {
    let queries = vec![
        r#" 
            SELECT actor_id, count(actor_id) from (
                SELECT actor_id, first_name, last_name from actor
            );
        "#,
        r#" 
            WITH tbl as (
                SELECT actor_id, first_name, last_name from actor
            ) 
            SELECT actor_id, count(actor_id) from tbl;
        "#,
    ];

    helper::compare_with_sqlite(
        &vec!["actor"],
        queries,
        TestInstruction::FromCsv("actor", vec!["actor"]),
    );
}
