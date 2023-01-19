use sqlparser::{dialect::AnsiDialect, parser::Parser};

use crate::pipeline::builder::{get_query, PipelineBuilder};

const SQL_FIRST_JOIN: &str = r#"
  SELECT
  a.name as "Genre",
    SUM(amount) as "Gross Revenue(in $)"
  FROM
  (	
    SELECT
    c.name, f.title, p.amount 
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

  GROUP BY name
  ORDER BY sum(amount) desc
  LIMIT 5;
"#;

const SQL_2: &str = r#"
    SELECT
    c.name, f.title, p.amount 
  FROM film f
  LEFT JOIN film_category fc
"#;

const SQL_3: &str = r#"
WITH tbl as (select id from a)    
select id from tbl
"#;

const SQL_4: &str = r#"
WITH tbl as (select id from a),
tbl2 as (select id from tbl)    
select id from tbl2
"#;
#[test]
fn test_1() {
    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, SQL_3).unwrap();

    let query = get_query(ast.get(0).unwrap().to_owned()).unwrap();

    println!("GROUPBY");

    println!("{:?}", query.group_by);

    println!("SELECT");

    println!("{:?}", query.projection);

    println!("FROM");
    println!("{:?}", query.from);

    println!("WITH");
    println!("{:?}", query.);
}

// #[test]
// fn test_2() {
//     let mut pipeline = PipelineBuilder {}
//         .build_pipeline(SQL_FIRST_JOIN)
//         .unwrap_or_else(|e| panic!("Unable to start the Executor: {}", e));

//         pipeline.
// }
