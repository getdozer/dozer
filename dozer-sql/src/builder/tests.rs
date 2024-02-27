use super::statement_to_pipeline;
use crate::{errors::PipelineError, tests::utils::create_test_runtime};
use dozer_core::app::AppPipeline;
#[test]
#[should_panic]
fn disallow_zero_outgoing_ndes() {
    let sql = "select * from film";
    let runtime = create_test_runtime();
    statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    )
    .unwrap();
}

#[test]
fn test_duplicate_into_clause() {
    let sql = "select * into table1 from film1 ; select * into table1 from film2";
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    assert!(matches!(
        result,
        Err(PipelineError::DuplicateIntoClause(dup_table)) if dup_table == "table1"
    ));
}

#[test]
fn parse_sql_pipeline() {
    let sql = r#"
            SELECT
                a.name as "Genre",
                SUM(amount) as "Gross Revenue(in $)"
            INTO gross_revenue_stats
            FROM
            (
                SELECT
                    c.name,
                    f.title,
                    p.amount
                FROM film f
                LEFT JOIN film_category fc
                    ON fc.film_id = f.film_id
                LEFT JOIN category c
                    ON fc.category_id = c.category_id
                LEFT JOIN inventory i
                    ON i.film_id = f.film_id
                LEFT JOIN rental r
                    ON r.inventory_id = i.inventory_id
                LEFT JOIN payment p
                    ON p.rental_id = r.rental_id
                WHERE p.amount IS NOT NULL
            ) a
            GROUP BY name;

            SELECT
            f.name, f.title, p.amount
            INTO film_amounts
            FROM film f
            LEFT JOIN film_category fc;

            WITH tbl as (select id from a)
            select id
            into cte_table
            from tbl;

            WITH tbl as (select id from  a),
            tbl2 as (select id from tbl)
            select id
            into nested_cte_table
            from tbl2;

            WITH cte_table1 as (select id_dt1 from (select id_t1 from table_1) as derived_table_1),
            cte_table2 as (select id_ct1 from cte_table1)
            select id_ct2
            into nested_derived_table
            from cte_table2;

            with tbl as (select id, ticker from stocks)
            select tbl.id
            into nested_stocks_table
            from  stocks join tbl on tbl.id = stocks.id;
        "#;

    let runtime = create_test_runtime();
    let context = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    )
    .unwrap();

    // Should create as many output tables as into statements
    let mut output_keys = context.output_tables_map.keys().collect::<Vec<_>>();
    output_keys.sort();
    let mut expected_keys = vec![
        "gross_revenue_stats",
        "film_amounts",
        "cte_table",
        "nested_cte_table",
        "nested_derived_table",
        "nested_stocks_table",
    ];
    expected_keys.sort();
    assert_eq!(output_keys, expected_keys);
}

#[test]
fn test_missing_into_in_simple_from_clause() {
    let sql = r#"SELECT a FROM B "#;
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    //check if the result is an error
    assert!(matches!(result, Err(PipelineError::MissingIntoClause)))
}

#[test]
fn test_correct_into_clause() {
    let sql = r#"SELECT a INTO C FROM B"#;
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    //check if the result is ok
    assert!(result.is_ok());
}

#[test]
fn test_missing_into_in_nested_from_clause() {
    let sql = r#"SELECT a FROM (SELECT a from b)"#;
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    //check if the result is an error
    assert!(matches!(result, Err(PipelineError::MissingIntoClause)))
}

#[test]
fn test_correct_into_in_nested_from() {
    let sql = r#"SELECT a INTO c FROM (SELECT a from b)"#;
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    //check if the result is ok
    assert!(result.is_ok());
}

#[test]
fn test_missing_into_in_with_clause() {
    let sql = r#"WITH tbl as (select a from B)
select B
from tbl;"#;
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    //check if the result is an error
    assert!(matches!(result, Err(PipelineError::MissingIntoClause)))
}

#[test]
fn test_correct_into_in_with_clause() {
    let sql = r#"WITH tbl as (select a from B)
select B
into C
from tbl;"#;
    let runtime = create_test_runtime();
    let result = statement_to_pipeline(
        sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
    );
    //check if the result is ok
    assert!(result.is_ok());
}
