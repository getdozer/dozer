use std::collections::HashSet;

use super::{
    helper::{self, get_sample_ops},
    TestInstruction,
};

#[test]
fn union_query() {
    let queries = r#" 
        WITH actor_id_union AS (
            SELECT actor_id
            FROM actor
            UNION
            SELECT actor_id
            FROM film_actor
        )
        SELECT actor_id
        INTO set_results
        FROM actor_id_union;
      "#;

    let table_names = vec!["actor", "film_actor"];
    let results = helper::query(
        &table_names,
        queries,
        TestInstruction::FromCsv("set_results", table_names.clone()),
    );

    let mut src_keys = HashSet::new();
    results.source_result.iter().for_each(|x| {
        src_keys.insert(x.values[0].to_int());
    });

    let mut dst_keys = HashSet::new();
    results.dest_result.iter().for_each(|x| {
        dst_keys.insert(x.values[0].to_int());
    });

    assert_eq!(src_keys.len(), dst_keys.len());

    let results = helper::query(
        &table_names,
        queries,
        TestInstruction::List(get_sample_ops()),
    );

    let mut src_keys = HashSet::new();
    results.source_result.iter().for_each(|x| {
        src_keys.insert(x.values[0].to_int());
    });

    let mut dst_keys = HashSet::new();
    results.dest_result.iter().for_each(|x| {
        dst_keys.insert(x.values[0].to_int());
    });

    assert_eq!(src_keys.len(), dst_keys.len());
}
