use super::{
    helper::{self},
    TestInstruction,
};

#[ignore]
#[test]
fn join_query() {
    let queries = vec![
        r#" 
        SELECT a.actor_id, first_name, last_name from actor a 
        JOIN film_actor fa on fa.actor_id = a.actor_id;
      "#,
    ];

    let table_names = vec!["actor", "film", "film_actor"];
    helper::compare_with_sqlite(
        &table_names,
        queries,
        TestInstruction::FromCsv("actor", table_names.clone()),
    );
}
