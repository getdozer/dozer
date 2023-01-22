use super::{
    helper::{self},
    TestInstruction,
};

#[test]
fn agg_query() {
    let queries = vec![
        r#" 
          SELECT film_id, count(film_id) from film
          GROUP By film_id;
        "#,
        r#"
          SELECT actor_id, count(actor_id) from actor
          GROUP By actor_id;
        "#,
    ];

    helper::compare_with_sqlite(
        &vec!["film", "actor"],
        queries,
        TestInstruction::FromCsv("actor", vec!["film", "actor"]),
    );
}
