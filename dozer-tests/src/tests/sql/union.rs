use crate::tests::sql::helper::get_sample_ops;
use std::collections::HashSet;

use super::{helper, TestInstruction};

#[test]
#[ignore]
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
        FROM actor_id_union;
      "#;

    let table_names = vec!["actor", "film_actor"];
    let results = helper::query(
        &table_names,
        queries,
        &None,
        &TestInstruction::FromCsv("actor", table_names.clone()),
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
        &None,
        &TestInstruction::List(get_sample_ops()),
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

#[test]
#[ignore]
fn union_all_query() {
    let queries = r#"
        WITH actor_id_union_all AS (
            SELECT actor_id
            FROM actor
            UNION ALL
            SELECT actor_id
            FROM film_actor
        )
        SELECT actor_id
        FROM actor_id_union_all;
      "#;

    let table_names = vec!["actor", "film_actor"];
    let results = helper::query(
        &table_names,
        queries,
        &None,
        &TestInstruction::FromCsv("actor", table_names.clone()),
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
        &None,
        &TestInstruction::List(get_sample_ops()),
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
