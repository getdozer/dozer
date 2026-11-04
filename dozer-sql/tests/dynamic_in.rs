#[path = "support/dynamic_in.rs"]
mod support;
use dozer_types::types::{Field, FieldType, Operation, Record};
use support::*;

#[test]
fn live_outer_and_inner_changes_reach_the_real_sink() {
    let mut steps = vec![
        step("outer arrives first", 0, insert(pair(1, 10)), vec![]),
        step(
            "first membership",
            1,
            insert(pair(1, 100)),
            vec![pair(1, 10)],
        ),
        step(
            "duplicate inner",
            1,
            insert(pair(1, 200)),
            vec![pair(1, 10)],
        ),
        step(
            "one duplicate removed",
            1,
            delete(pair(1, 100)),
            vec![pair(1, 10)],
        ),
        step("last duplicate removed", 1, delete(pair(1, 200)), vec![]),
        step(
            "membership restored",
            1,
            insert(pair(1, 300)),
            vec![pair(1, 10)],
        ),
        step(
            "matching outer payload update",
            0,
            update(pair(1, 10), pair(1, 11)),
            vec![pair(1, 11)],
        ),
        step(
            "outer leaves membership",
            0,
            update(pair(1, 11), pair(2, 20)),
            vec![],
        ),
        step(
            "outer remains excluded",
            0,
            update(pair(2, 20), pair(3, 30)),
            vec![],
        ),
        step(
            "outer enters membership",
            0,
            update(pair(3, 30), pair(1, 12)),
            vec![pair(1, 12)],
        ),
        step(
            "inner payload-only update",
            1,
            update(pair(1, 300), pair(1, 301)),
            vec![pair(1, 12)],
        ),
        step(
            "inner key update retracts",
            1,
            update(pair(1, 301), pair(2, 301)),
            vec![],
        ),
        step(
            "matching outer delete after exclusion",
            0,
            delete(pair(1, 12)),
            vec![],
        ),
        step(
            "inner arrived before outer",
            0,
            insert(pair(2, 22)),
            vec![pair(2, 22)],
        ),
        step("outer delete", 0, delete(pair(2, 22)), vec![]),
    ];
    steps[2].emitted = Some(vec![]);
    steps[3].emitted = Some(vec![]);
    steps[6].emitted = Some(vec![update(pair(1, 10), pair(1, 11))]);
    steps[10].emitted = Some(vec![]);
    let schema = run(
        "SELECT * FROM outer_rows WHERE key IN (SELECT key FROM members)",
        tables(),
        steps,
        true,
        false,
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["key", "payload"]
    );
}

#[test]
fn duplicates_and_batches_keep_outer_multiplicity() {
    run(
        "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members)",
        tables(),
        vec![
            step(
                "duplicate outer batch",
                0,
                Operation::BatchInsert {
                    new: vec![pair(1, 10), pair(1, 10), pair(2, 20)],
                },
                vec![],
            ),
            step(
                "inner duplicate batch",
                1,
                Operation::BatchInsert {
                    new: vec![pair(1, 1), pair(1, 2)],
                },
                vec![row(1), row(1)],
            ),
            step("delete one outer", 0, delete(pair(1, 10)), vec![row(1)]),
            step("delete one inner", 1, delete(pair(1, 1)), vec![row(1)]),
            step("delete last inner", 1, delete(pair(1, 2)), vec![]),
            step(
                "remaining outer can return",
                1,
                insert(pair(1, 3)),
                vec![row(1)],
            ),
        ],
        true,
        false,
    )
    .unwrap();
}

#[test]
fn not_in_empty_null_and_live_null_removal() {
    let null_pair = Record::new(vec![Field::Null, Field::Int(0)]);
    run(
        "SELECT key FROM outer_rows WHERE key NOT IN (SELECT key FROM members)",
        tables(),
        vec![
            step(
                "NULL outer with empty RHS",
                0,
                insert(null_pair.clone()),
                vec![null()],
            ),
            step(
                "non-NULL outer with empty RHS",
                0,
                insert(pair(2, 0)),
                vec![null(), row(2)],
            ),
            step(
                "nonempty RHS makes NULL outer unknown",
                1,
                insert(pair(1, 0)),
                vec![row(2)],
            ),
            step(
                "NULL RHS makes unmatched outer unknown",
                1,
                insert(null_pair.clone()),
                vec![],
            ),
            step(
                "last NULL removed",
                1,
                delete(null_pair.clone()),
                vec![row(2)],
            ),
            step(
                "last RHS removed",
                1,
                delete(pair(1, 0)),
                vec![null(), row(2)],
            ),
            step("NULL-only RHS", 1, insert(null_pair.clone()), vec![]),
            step(
                "NULL updated to matching value",
                1,
                update(null_pair, pair(2, 0)),
                vec![],
            ),
            step(
                "empty RHS restored",
                1,
                delete(pair(2, 0)),
                vec![null(), row(2)],
            ),
        ],
        true,
        false,
    )
    .unwrap();
}

#[test]
fn compound_multiple_and_nested_expressions() {
    run("SELECT key FROM outer_rows WHERE NOT (key IN (SELECT key FROM members) AND payload = 0) OR key IN (SELECT key FROM others)", tables(), vec![
        step("outer excluded by first input", 1, insert(pair(1, 0)), vec![]),
        step("outer", 0, insert(pair(1, 0)), vec![]),
        step("second membership overrides", 2, insert(pair(1, 0)), vec![row(1)]),
        step("second membership removed", 2, delete(pair(1, 0)), vec![]),
        step("first becomes unknown", 1, update(pair(1, 0), Record::new(vec![Field::Null, Field::Int(0)])), vec![]),
        step("false AND unknown negates to true", 0, update(pair(1, 0), pair(1, 1)), vec![row(1)]),
    ], true, false).unwrap();
    run("SELECT key FROM outer_rows WHERE CASE WHEN (key IN (SELECT key FROM members)) IS TRUE THEN 7 ELSE 0 END = 7", tables(), vec![
        step("outer", 0, insert(pair(1, 0)), vec![]),
        step("case becomes true", 1, insert(pair(1, 0)), vec![row(1)]),
        step("case becomes false", 1, delete(pair(1, 0)), vec![]),
    ], true, false).unwrap();
    let null_member = Record::new(vec![Field::Null, Field::Int(0)]);
    run("SELECT key FROM outer_rows WHERE CAST(key IN (SELECT key FROM members) AS STRING) = 'TRUE'", tables(), vec![
        step("cast starts false with empty membership", 0, insert(pair(1, 0)), vec![]),
        step("cast handles unknown membership", 1, insert(null_member.clone()), vec![]),
        step("cast selects a true match", 1, insert(pair(1, 0)), vec![row(1)]),
        step("cast retracts when match becomes unknown", 1, delete(pair(1, 0)), vec![]),
        step("cast returns to empty false membership", 1, delete(null_member), vec![]),
        step("cast selects restored membership", 1, insert(pair(1, 0)), vec![row(1)]),
        step("cast forwards outer removal", 0, delete(pair(1, 0)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn scalar_projection_filter_alias_and_derived_table() {
    run("SELECT o.key FROM outer_rows o WHERE o.key IN (SELECT d.shifted FROM (SELECT m.key + 1 AS shifted FROM members m WHERE m.payload > 0) d)", tables(), vec![
        step("outer", 0, insert(pair(2, 0)), vec![]),
        step("filtered inner", 1, insert(pair(1, 0)), vec![]),
        step("inner enters filter", 1, update(pair(1, 0), pair(1, 1)), vec![row(2)]),
        step("projection changes", 1, update(pair(1, 1), pair(3, 1)), vec![]),
        step("outer matches projected value", 0, update(pair(2, 0), pair(4, 0)), vec![row(4)]),
        step("inner leaves filter", 1, update(pair(3, 1), pair(3, 0)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn inner_join_changes_drive_membership() {
    run("SELECT key FROM outer_rows WHERE key IN (SELECT m.key FROM members m JOIN others x ON m.key = x.key)", tables(), vec![
        step("outer", 0, insert(pair(1, 0)), vec![]),
        step("one join side", 1, insert(pair(1, 0)), vec![]),
        step("join becomes populated", 2, insert(pair(1, 5)), vec![row(1)]),
        step("duplicate join match", 2, insert(pair(1, 6)), vec![row(1)]),
        step("one match deleted", 2, delete(pair(1, 5)), vec![row(1)]),
        step("last match leaves", 2, update(pair(1, 6), pair(2, 6)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn inner_aggregation_and_having_updates_are_live() {
    run("SELECT key FROM outer_rows WHERE key IN (SELECT COUNT(key) FROM members GROUP BY payload HAVING COUNT(key) > 1)", tables(), vec![
        step("outer batch", 0, Operation::BatchInsert { new: vec![pair(1, 0), pair(2, 0), pair(3, 0)] }, vec![]),
        step("count one excluded", 1, insert(pair(10, 7)), vec![]),
        step("count two enters HAVING", 1, insert(pair(11, 7)), vec![row(2)]),
        step("count three changes membership", 1, insert(pair(12, 7)), vec![row(3)]),
        step("count two restored", 1, delete(pair(11, 7)), vec![row(2)]),
        step("count one leaves HAVING", 1, delete(pair(12, 7)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn inherited_cte_and_local_shadowing_remain_scoped() {
    run("WITH eligible AS (SELECT key FROM members WHERE payload > 0) SELECT key FROM outer_rows WHERE key IN (SELECT key FROM eligible) AND key NOT IN (WITH eligible AS (SELECT key FROM others) SELECT key FROM eligible)", tables(), vec![
        step("outer", 0, insert(pair(1, 0)), vec![]),
        step("inherited CTE", 1, insert(pair(1, 1)), vec![row(1)]),
        step("local CTE shadows inherited", 2, insert(pair(1, 0)), vec![]),
        step("local CTE retracts", 2, delete(pair(1, 0)), vec![row(1)]),
        step("inherited CTE retracts", 1, delete(pair(1, 1)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn nested_membership_and_parenthesized_queries() {
    run("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members WHERE key IN (SELECT key FROM others WHERE key NOT IN (SELECT key FROM more)))", tables(), vec![
        step("outer", 0, insert(pair(1, 0)), vec![]),
        step("first level", 1, insert(pair(1, 0)), vec![]),
        step("second level", 2, insert(pair(1, 0)), vec![row(1)]),
        step("third level excludes", 3, insert(pair(1, 0)), vec![]),
        step("third level retracts", 3, delete(pair(1, 0)), vec![row(1)]),
        step("second level retracts", 2, delete(pair(1, 0)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn union_all_and_distinct_membership_retract_last_copy() {
    for quantifier in ["ALL", "DISTINCT", ""] {
        run(&format!("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members UNION {quantifier} SELECT key FROM others)"), tables(), vec![
            step("outer", 0, insert(pair(1, 0)), vec![]),
            step("left union", 1, insert(pair(1, 1)), vec![row(1)]),
            step("right union duplicate", 2, insert(pair(1, 2)), vec![row(1)]),
            step("left union retracts", 1, delete(pair(1, 1)), vec![row(1)]),
            step("right union retracts", 2, delete(pair(1, 2)), vec![]),
        ], true, false).unwrap_or_else(|error| panic!("UNION {quantifier}: {error}"));
    }
}

#[test]
fn parenthesized_cte_not_in_retains_duplicates_and_null_membership() {
    let null_member = Record::new(vec![Field::Null, Field::Int(0)]);
    run("SELECT key FROM outer_rows WHERE key NOT IN (/* query */ WITH x AS (SELECT key FROM members) SELECT key FROM x)", tables(), vec![
        step("outer duplicates with empty membership", 0, Operation::BatchInsert { new: vec![pair(1, 0), pair(1, 0)] }, vec![row(1), row(1)]),
        step("first member", 1, insert(pair(1, 0)), vec![]),
        step("duplicate member", 1, insert(pair(1, 0)), vec![]),
        step("one copy remains", 1, delete(pair(1, 0)), vec![]),
        step("last copy removed", 1, delete(pair(1, 0)), vec![row(1), row(1)]),
        step("NULL makes nonmatches unknown", 1, insert(null_member.clone()), vec![]),
        step("NULL removed", 1, delete(null_member), vec![row(1), row(1)]),
    ], true, false).unwrap();
}

#[test]
fn review_qualified_cte_outputs_rebind_to_their_local_relation() {
    for sql in [
        "WITH c AS (SELECT key FROM members) SELECT key FROM outer_rows WHERE key IN (SELECT c.key FROM c)",
        "SELECT key FROM outer_rows WHERE key IN (WITH c AS (SELECT key FROM members) SELECT c.key FROM c)",
        "WITH c AS (SELECT key FROM members UNION ALL SELECT key FROM others) SELECT key FROM outer_rows WHERE key IN (SELECT c.key FROM c)",
    ] {
        run(sql, tables(), vec![
            step("outer", 0, insert(pair(1, 0)), vec![]),
            step("qualified CTE member", 1, insert(pair(1, 0)), vec![row(1)]),
            step("qualified CTE retraction", 1, delete(pair(1, 0)), vec![]),
        ], true, false).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    for source in [
        "c JOIN others t ON c.key = t.key",
        "others t JOIN c ON t.key = c.key",
    ] {
        let sql = format!("WITH c AS (SELECT key FROM members) SELECT key FROM outer_rows WHERE key IN (SELECT c.key FROM {source})");
        run(
            &sql,
            tables(),
            vec![
                step("outer", 0, insert(pair(1, 0)), vec![]),
                step("CTE side", 1, insert(pair(1, 0)), vec![]),
                step("join side", 2, insert(pair(1, 0)), vec![row(1)]),
                step("CTE side removed", 1, delete(pair(1, 0)), vec![]),
            ],
            true,
            false,
        )
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn review_union_sibling_queries_keep_independent_cte_names() {
    for prefix in ["", "WITH x AS (SELECT key FROM more) "] {
        let sql = format!("{prefix}SELECT key FROM outer_rows WHERE key IN (SELECT a.key FROM (WITH x AS (SELECT key FROM members) SELECT x.key FROM x) AS a UNION ALL SELECT b.key FROM (WITH x AS (SELECT key FROM others) SELECT x.key FROM x) AS b)");
        run(
            &sql,
            tables(),
            vec![
                step("outer", 0, insert(pair(1, 0)), vec![]),
                step("left local x", 1, insert(pair(1, 0)), vec![row(1)]),
                step("right local x", 2, insert(pair(1, 0)), vec![row(1)]),
                step("left x removed", 1, delete(pair(1, 0)), vec![row(1)]),
                step("right x removed", 2, delete(pair(1, 0)), vec![]),
            ],
            true,
            false,
        )
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    // A left branch definition is not visible to a sibling without its own WITH.
    assert!(run("SELECT key FROM outer_rows WHERE key IN (SELECT a.key FROM (WITH x AS (SELECT key FROM members) SELECT key FROM x) AS a UNION ALL SELECT key FROM x)", tables(), vec![], false, false).is_err());
}

#[test]
fn review_window_operators_retain_and_use_their_cte_input() {
    let timestamp = Field::Timestamp("2026-01-01T00:02:00+00:00".parse().unwrap());
    let input = Record::new(vec![Field::Int(1), timestamp]);
    let source_schema = schema(&[("key", FieldType::Int), ("ts", FieldType::Timestamp)]);
    for (operator, copies) in [
        ("TUMBLE(c, ts, '5 MINUTES')", 1),
        ("HOP(c, ts, '5 MINUTES', '10 MINUTES')", 2),
    ] {
        for physical_collision in [false, true] {
            let mut sources = vec![
                ("outer_rows".into(), source_schema.clone()),
                ("members".into(), schema(&[("key", FieldType::Int)])),
            ];
            if physical_collision {
                sources.push(("c".into(), source_schema.clone()));
            }
            // The key transformation proves the CTE stream is consumed even
            // when an unrelated physical source also happens to be named c.
            let sql = format!("WITH c AS (SELECT key + 10 AS key, ts FROM outer_rows) SELECT key FROM {operator} w WHERE key IN (SELECT key FROM members)");
            run(
                &sql,
                sources,
                vec![
                    step("member before window input", 1, insert(row(11)), vec![]),
                    step(
                        "transformed CTE enters window",
                        0,
                        insert(input.clone()),
                        vec![row(11); copies],
                    ),
                    step(
                        "membership retracts window rows",
                        1,
                        delete(row(11)),
                        vec![],
                    ),
                    step(
                        "membership restores window rows",
                        1,
                        insert(row(11)),
                        vec![row(11); copies],
                    ),
                    step(
                        "CTE deletion retracts window rows",
                        0,
                        delete(input.clone()),
                        vec![],
                    ),
                ],
                true,
                false,
            )
            .unwrap_or_else(|error| {
                panic!("{sql}, physical collision={physical_collision}: {error}")
            });
        }
    }
}

#[test]
fn review_ttl_and_nested_operator_cte_inputs_build_without_external_cte_source() {
    for operator in [
        "TTL(c, ts, '1 HOUR')",
        "TUMBLE(TTL(c, ts, '1 HOUR'), ts, '5 MINUTES')",
        "TTL(HOP(c, ts, '5 MINUTES', '10 MINUTES'), ts, '1 HOUR')",
    ] {
        let sql = format!("WITH c AS (SELECT key, ts FROM outer_rows) SELECT key FROM {operator} w WHERE key IN (SELECT key FROM members)");
        println!("Building CTE-backed table operator: {sql}");
        run(
            &sql,
            vec![
                (
                    "outer_rows".into(),
                    schema(&[("key", FieldType::Int), ("ts", FieldType::Timestamp)]),
                ),
                ("members".into(), schema(&[("key", FieldType::Int)])),
            ],
            vec![],
            false,
            false,
        )
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn review_window_join_cte_inputs_reach_both_sides_and_chained_joins() {
    let input = Record::new(vec![
        Field::Int(1),
        Field::Timestamp("2026-01-01T00:02:00+00:00".parse().unwrap()),
    ]);
    let source_schema = schema(&[("key", FieldType::Int), ("ts", FieldType::Timestamp)]);
    for (operator, copies) in [
        ("TUMBLE(c, ts, '5 MINUTES')", 1),
        ("HOP(c, ts, '5 MINUTES', '10 MINUTES')", 2),
    ] {
        for operator_on_left in [true, false] {
            for chained in [false, true] {
                let mut from = if operator_on_left {
                    format!("{operator} w JOIN others o ON w.key = o.key")
                } else {
                    format!("others o JOIN {operator} w ON o.key = w.key")
                };
                if chained {
                    from.push_str(" JOIN more x ON o.key = x.key");
                }
                let sql = format!("WITH c AS (SELECT key + 10 AS key, ts FROM outer_rows) SELECT w.key FROM {from} WHERE w.key IN (SELECT key FROM members)");
                println!("Running CTE-backed joined window: {sql}");
                let mut steps = vec![
                    step("member before join input", 1, insert(row(11)), vec![]),
                    step(
                        "transformed CTE window awaits join",
                        0,
                        insert(input.clone()),
                        vec![],
                    ),
                    step(
                        "other join side",
                        2,
                        insert(row(11)),
                        if chained {
                            vec![]
                        } else {
                            vec![row(11); copies]
                        },
                    ),
                ];
                if chained {
                    steps.push(step(
                        "final chained join side",
                        3,
                        insert(row(11)),
                        vec![row(11); copies],
                    ));
                }
                steps.extend([
                    step(
                        "membership retracts joined windows",
                        1,
                        delete(row(11)),
                        vec![],
                    ),
                    step(
                        "membership restores joined windows",
                        1,
                        insert(row(11)),
                        vec![row(11); copies],
                    ),
                    step(
                        "join side deletion retracts windows",
                        2,
                        delete(row(11)),
                        vec![],
                    ),
                    step(
                        "join side restored",
                        2,
                        insert(row(11)),
                        vec![row(11); copies],
                    ),
                    step(
                        "CTE deletion retracts joined windows",
                        0,
                        delete(input.clone()),
                        vec![],
                    ),
                ]);
                run(
                    &sql,
                    vec![
                        ("outer_rows".into(), source_schema.clone()),
                        ("members".into(), schema(&[("key", FieldType::Int)])),
                        ("others".into(), schema(&[("key", FieldType::Int)])),
                        ("more".into(), schema(&[("key", FieldType::Int)])),
                        // The CTE must win over this unrelated physical source.
                        ("c".into(), source_schema.clone()),
                    ],
                    steps,
                    true,
                    false,
                )
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            }
        }
    }
}

#[test]
fn review_nested_operator_join_cte_inputs_build_on_both_sides() {
    for operator in [
        "TTL(c, ts, '1 HOUR')",
        "TUMBLE(TTL(c, ts, '1 HOUR'), ts, '5 MINUTES')",
        "TTL(HOP(c, ts, '5 MINUTES', '10 MINUTES'), ts, '1 HOUR')",
    ] {
        for operator_on_left in [true, false] {
            for chained in [false, true] {
                let mut from = if operator_on_left {
                    format!("{operator} w JOIN others o ON w.key = o.key")
                } else {
                    format!("others o JOIN {operator} w ON o.key = w.key")
                };
                if chained {
                    from.push_str(" JOIN more x ON o.key = x.key");
                }
                let sql = format!("WITH c AS (SELECT key, ts FROM outer_rows) SELECT w.key FROM {from} WHERE w.key IN (SELECT key FROM members)");
                println!("Building CTE-backed joined operator: {sql}");
                run(
                    &sql,
                    vec![
                        (
                            "outer_rows".into(),
                            schema(&[("key", FieldType::Int), ("ts", FieldType::Timestamp)]),
                        ),
                        ("members".into(), schema(&[("key", FieldType::Int)])),
                        ("others".into(), schema(&[("key", FieldType::Int)])),
                        ("more".into(), schema(&[("key", FieldType::Int)])),
                    ],
                    vec![],
                    false,
                    false,
                )
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            }
        }
    }
}

#[test]
fn review_nested_distinct_preserves_multiplicity_before_aggregation() {
    for inner in [
        "SELECT DISTINCT key FROM members",
        "SELECT DISTINCT payload AS key FROM members GROUP BY key, payload",
    ] {
        let sql = format!(
            "SELECT key FROM outer_rows WHERE key IN (SELECT COUNT(d.key) FROM ({inner}) d)"
        );
        let first = pair(7, 7);
        let second = if inner.contains("GROUP BY") {
            pair(8, 7)
        } else {
            pair(7, 8)
        };
        let mut steps = vec![
            step(
                "outer alternatives",
                0,
                Operation::BatchInsert {
                    new: vec![pair(1, 0), pair(2, 0)],
                },
                vec![],
            ),
            step("one distinct value", 1, insert(first.clone()), vec![row(1)]),
            step(
                "duplicate projected value",
                1,
                insert(second.clone()),
                vec![row(1)],
            ),
            step("one duplicate remains", 1, delete(first), vec![row(1)]),
            step("last distinct value removed", 1, delete(second), vec![]),
        ];
        steps[2].emitted = Some(vec![]);
        steps[3].emitted = Some(vec![]);
        run(&sql, tables(), steps, true, false).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn review_inherited_cte_relational_clauses_fail_before_ingestion() {
    for (definition, expected) in [
        ("SELECT key FROM members FETCH FIRST 1 ROW ONLY", "FETCH"),
        (
            "SELECT key FROM members FOR UPDATE",
            "Expected ), found: UPDATE",
        ),
        (
            "SELECT key FROM members WHERE NOW() IS NOT NULL",
            "Volatile",
        ),
    ] {
        let sql = format!("WITH c AS ({definition}) SELECT key FROM outer_rows WHERE key IN (SELECT c.key FROM c)");
        let error = run(&sql, tables(), vec![], false, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{sql}: {error}");
    }
}

#[test]
fn review_union_membership_updates_preserve_atomic_and_noop_behavior() {
    for quantifier in ["ALL", "DISTINCT", ""] {
        let sql = format!("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members UNION {quantifier} SELECT key FROM others)");
        let mut steps = vec![
            step(
                "outer alternatives",
                0,
                Operation::BatchInsert {
                    new: vec![pair(1, 10), pair(2, 20)],
                },
                vec![],
            ),
            step("first member", 1, insert(pair(1, 100)), vec![row(1)]),
            step(
                "payload update leaves membership unchanged",
                1,
                update(pair(1, 100), pair(1, 101)),
                vec![row(1)],
            ),
            step(
                "key update changes membership",
                1,
                update(pair(1, 101), pair(2, 101)),
                vec![row(2)],
            ),
            step(
                "another old-key copy",
                2,
                insert(pair(1, 200)),
                vec![row(1), row(2)],
            ),
            step(
                "key update converges on an existing member",
                1,
                update(pair(2, 101), pair(1, 102)),
                vec![row(1)],
            ),
            step(
                "one UNION copy removed",
                2,
                delete(pair(1, 200)),
                vec![row(1)],
            ),
            step("last copy removed", 1, delete(pair(1, 102)), vec![]),
        ];
        steps[2].emitted = Some(vec![]);
        steps[3].emitted = Some(vec![delete(row(1)), insert(row(2))]);
        steps[5].emitted = Some(vec![delete(row(2))]);
        steps[6].emitted = Some(vec![]);
        run(&sql, tables(), steps, true, false).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn union_branch_schemas_are_checked_before_ingestion() {
    for right in [
        schema(&[("key", FieldType::String)]),
        schema(&[("key", FieldType::Int), ("extra", FieldType::Int)]),
    ] {
        let mut sources = tables();
        sources[2].1 = right;
        let error = run(
            "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members UNION ALL SELECT * FROM others)",
            sources, vec![], false, false,
        ).unwrap_err().to_string();
        assert!(error.contains("Invalid input schemas"), "{error}");
    }
}

#[test]
fn union_right_alias_and_nullable_membership_reach_the_sink() {
    let mut sources = tables();
    sources[1].1.fields[0].nullable = false;
    let null_member = Record::new(vec![Field::Null, Field::Int(0)]);
    run(
        "SELECT key FROM outer_rows WHERE key NOT IN (SELECT key AS left_key FROM members UNION ALL SELECT key AS right_key FROM others)",
        sources,
        vec![
            step("outer with empty union", 0, insert(pair(1, 0)), vec![row(1)]),
            step("nullable right branch", 2, insert(null_member.clone()), vec![]),
            step("left branch match", 1, insert(pair(1, 0)), vec![]),
            step("right NULL removed", 2, delete(null_member), vec![]),
            step("last left value removed", 1, delete(pair(1, 0)), vec![row(1)]),
        ],
        true,
        false,
    ).unwrap();
}

#[test]
fn same_source_can_feed_both_sides_without_id_collisions() {
    run(
        "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM outer_rows WHERE payload > 0)",
        tables(),
        vec![
            step(
                "row does not qualify as member",
                0,
                insert(pair(1, 0)),
                vec![],
            ),
            step(
                "same source establishes membership",
                0,
                insert(pair(1, 1)),
                vec![row(1), row(1)],
            ),
            step("qualifying row removed", 0, delete(pair(1, 1)), vec![]),
        ],
        true,
        false,
    )
    .unwrap();
}

#[test]
fn original_static_in_null_semantics_remain_unchanged() {
    let value = Record::new(vec![Field::Null, Field::Int(0)]);
    run(
        "SELECT key FROM outer_rows WHERE key IN (NULL)",
        tables(),
        vec![step(
            "static NULL equality",
            0,
            insert(value.clone()),
            vec![null()],
        )],
        true,
        false,
    )
    .unwrap();
    run(
        "SELECT key FROM outer_rows WHERE key IN (NULL) OR key IN (SELECT key FROM members)",
        tables(),
        vec![step(
            "static NULL equality inside dynamic filter",
            0,
            insert(value),
            vec![null()],
        )],
        true,
        false,
    )
    .unwrap();
}

#[test]
fn unsupported_subqueries_fail_explicitly_before_ingestion() {
    for (sql, expected) in [
        ("SELECT o.key FROM outer_rows o WHERE o.key IN (SELECT m.key FROM members m WHERE m.key = o.key)", "Correlated or unknown qualifier"),
        ("SELECT o.key FROM outer_rows o WHERE o.key IN (SELECT o.key FROM members m)", "Correlated or unknown qualifier"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key, payload FROM members)", "exactly one column"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members ORDER BY key)", "ORDER BY"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members LIMIT 1)", "Limit and Offset"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members FETCH FIRST 1 ROW ONLY)", "FETCH"),
        ("SELECT key FROM outer_rows WHERE key IN (WITH RECURSIVE x AS (SELECT key FROM members) SELECT key FROM x)", "Recursive"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members, others)", "Unsupported SELECT"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM LATERAL (SELECT key FROM members) x)", "lateral"),
        ("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members INTERSECT SELECT key FROM others)", "SELECT and UNION"),
    ] {
        let error = run(sql, tables(), vec![], false, false).unwrap_err().to_string();
        assert!(error.contains(expected), "expected {expected:?}, got {error:?} for {sql}");
    }
    let mut types = tables();
    types[1].1 = schema(&[("key", FieldType::Timestamp)]);
    assert!(run(
        "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members)",
        types,
        vec![],
        false,
        false
    )
    .unwrap_err()
    .to_string()
    .contains("incompatible"));
    let mut missing = tables();
    missing[1].1 = schema(&[("key", FieldType::Int)]);
    assert!(run(
        "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members WHERE payload > 0)",
        missing,
        vec![],
        false,
        false
    )
    .is_err());
    assert!(run(
        "SELECT key FROM outer_rows WHERE (key, payload) IN (SELECT key FROM members)",
        tables(),
        vec![],
        false,
        false
    )
    .is_err());
    let mut qualified = tables();
    qualified[1].1 = schema(&[("key", FieldType::Int)]);
    let error = run("SELECT key FROM outer_rows WHERE key IN (SELECT m.payload FROM members m JOIN others x ON m.key = x.key)", qualified, vec![], false, false).unwrap_err().to_string();
    assert!(error.contains("Qualified column"), "{error}");
    let error = run(
        "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members WHERE NOW() IS NOT NULL)",
        tables(),
        vec![],
        false,
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("Volatile"), "{error}");
    let error = run("WITH c AS (SELECT missing.key FROM members) SELECT key FROM outer_rows WHERE key IN (SELECT key FROM c)", tables(), vec![], false, false).unwrap_err().to_string();
    assert!(
        error.contains("Correlated or unknown qualifier \"missing\""),
        "{error}"
    );
}

#[test]
fn numeric_schema_compatibility_and_parenthesized_union() {
    let mut types = tables();
    types[1].1 = schema(&[("key", FieldType::UInt), ("payload", FieldType::Int)]);
    run(
        "SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members)",
        types,
        vec![
            step("signed outer", 0, insert(pair(1, 0)), vec![]),
            step(
                "unsigned matching value",
                1,
                insert(Record::new(vec![Field::UInt(1), Field::Int(0)])),
                vec![row(1)],
            ),
        ],
        true,
        false,
    )
    .unwrap();
    run("SELECT key FROM outer_rows WHERE key IN (SELECT key FROM members UNION ALL SELECT key FROM others)", tables(), vec![
        step("outer", 0, insert(pair(1, 0)), vec![]),
        step("parenthesized union branch", 1, insert(pair(1, 0)), vec![row(1)]),
        step("other parenthesized branch", 2, insert(pair(1, 0)), vec![row(1)]),
        step("left branch retracts", 1, delete(pair(1, 0)), vec![row(1)]),
        step("last branch retracts", 2, delete(pair(1, 0)), vec![]),
    ], true, false).unwrap();
}

#[test]
fn real_dag_matches_independent_multiset_model_after_each_event() {
    for negated in [false, true] {
        let mut outer = Vec::<Record>::new();
        let mut inner = Vec::<Record>::new();
        let mut steps = vec![];
        let mut seed = 0x6001659_u64;
        for _ in 0..80 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let port = ((seed >> 32) & 1) as u16;
            let rows = if port == 0 { &mut outer } else { &mut inner };
            let key = match (seed >> 40) % 5 {
                0 => Field::Null,
                value => Field::Int(value as i64),
            };
            let value = Record::new(vec![key, Field::Int(0)]);
            let action = (seed >> 48) % 4;
            let operation = if rows.is_empty() || action == 0 {
                rows.push(value.clone());
                insert(value)
            } else if action == 1 {
                let old = rows.remove((seed as usize) % rows.len());
                delete(old)
            } else if action == 2 {
                let index = (seed as usize) % rows.len();
                let old = std::mem::replace(&mut rows[index], value.clone());
                update(old, value)
            } else {
                rows.extend([value.clone(), value.clone()]);
                Operation::BatchInsert {
                    new: vec![value.clone(), value],
                }
            };
            let expected = outer
                .iter()
                .filter(|value| {
                    let key = &value.values[0];
                    let truth = if inner.is_empty() {
                        Some(false)
                    } else if key == &Field::Null {
                        None
                    } else if inner.iter().any(|row| &row.values[0] == key) {
                        Some(true)
                    } else if inner.iter().any(|row| row.values[0] == Field::Null) {
                        None
                    } else {
                        Some(false)
                    };
                    truth.map(|truth| truth != negated) == Some(true)
                })
                .map(|row| Record::new(vec![row.values[0].clone()]))
                .collect();
            steps.push(step("model transition", port, operation, expected));
        }
        run(
            &format!(
                "SELECT key FROM outer_rows WHERE key {}IN (SELECT key FROM members)",
                if negated { "NOT " } else { "" }
            ),
            tables(),
            steps,
            true,
            false,
        )
        .unwrap();
    }
}
