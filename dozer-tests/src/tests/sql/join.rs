use super::{
    helper::{self, get_sample_ops},
    TestInstruction,
};

#[test]
fn join_query() {
    let queries = vec![
        r#" 
        SELECT actor.actor_id, first_name, last_name from actor 
        JOIN film_actor on film_actor.actor_id = actor.actor_id;
      "#,
    ];

    let table_names = vec!["actor", "film_actor", "film"];
    helper::compare_with_sqlite(
        &table_names,
        queries.clone(),
        TestInstruction::FromCsv("actor", table_names.clone()),
    );

    helper::compare_with_sqlite(
        &table_names,
        queries,
        TestInstruction::List(get_sample_ops()),
    );
}

#[test]
fn join_alias_query() {
    let queries = vec![
        r#" 
        SELECT a.actor_id, first_name, last_name from actor a 
        JOIN film_actor fa on fa.actor_id = a.actor_id;
      "#,
    ];

    let table_names = vec!["actor", "film_actor", "film"];
    helper::compare_with_sqlite(
        &table_names,
        queries.clone(),
        TestInstruction::FromCsv("actor", table_names.clone()),
    );
    helper::compare_with_sqlite(
        &table_names,
        queries,
        TestInstruction::List(get_sample_ops()),
    );
}

#[test]
#[ignore = "CTE alias currently not supported with JOIN"]
fn join_cte_query() {
    let queries = vec![
        r#" 
        WITH tbl as (
            SELECT actor_id, first_name, last_name from actor
        ) 
        SELECT tbl.actor_id, first_name, last_name from tbl 
        JOIN film_actor fa on fa.actor_id = tbl.actor_id;
      "#,
    ];

    let table_names = vec!["actor", "film_actor", "film"];
    helper::compare_with_sqlite(
        &table_names,
        queries,
        TestInstruction::FromCsv("actor", table_names.clone()),
    );
}
