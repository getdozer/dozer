use super::{
    helper::{self, get_sample_queries},
    TestInstruction,
};

#[test]
fn insert_only_queries() {
    let queries = get_sample_queries();
    helper::compare_with_sqlite(
        &vec!["actor"],
        queries,
        TestInstruction::FromCsv("actor", vec!["actor"]),
    );
}

#[test]
fn nullable_queries() {
    let list = vec![
            (
                "actor",
                "INSERT INTO actor(actor_id,first_name) values (1, 'mario')".to_string(),
            ),
            (
                "actor",
                "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (2, 'dario', null, null)".to_string(),
            ),
            (
                "actor",
                "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (3, 'luigi', null, null)".to_string(),
            ),
        ];
    let queries = get_sample_queries();
    helper::compare_with_sqlite(&vec!["actor"], queries, TestInstruction::List(list));
}

#[test]
fn changes_queries() {
    let queries =
        vec!["select actor_id, first_name, last_name,last_update from actor order by actor_id"];

    let list = helper::get_sample_ops();

    helper::compare_with_sqlite(&vec!["actor"], queries, TestInstruction::List(list));
}
