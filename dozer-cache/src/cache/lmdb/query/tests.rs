#[tokio::test]
async fn insert_and_query_record() -> anyhow::Result<()> {
    let val = "bar".to_string();
    let (cache, schema) = _setup().await;
    let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);

    cache.insert_with_schema(&record, &schema, "docs")?;

    // Query with an expression
    let exp = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Field::String("bar".to_string()),
        )),
        vec![],
        10,
        0,
    );

    query_and_test(&cache, &record, "docs", &exp)?;

    // Query without an expression
    query_and_test(
        &cache,
        &record,
        "docs",
        &QueryExpression::new(None, vec![], 10, 0),
    )?;

    Ok(())
}