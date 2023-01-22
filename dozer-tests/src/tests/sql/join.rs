use super::{
    helper::{self},
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
        queries,
        TestInstruction::FromCsv("actor", table_names.clone()),
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
        queries,
        TestInstruction::FromCsv("actor", table_names.clone()),
    );
}
