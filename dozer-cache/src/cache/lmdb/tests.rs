use std::sync::Arc;

use anyhow::{Context, Ok};
use dozer_schema::{
    registry::{_serve_channel, client, SchemaRegistryClient},
    test_helper::init_schema,
};
use dozer_types::types::{Field, Record, Schema};

use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    helper, Cache,
};

use super::cache::LmdbCache;

async fn _setup() -> (LmdbCache, Schema) {
    let client_transport = _serve_channel().unwrap();
    let client =
        Arc::new(SchemaRegistryClient::new(client::Config::default(), client_transport).spawn());
    let schema = init_schema(client.clone()).await;
    let cache = LmdbCache::new(true);
    (cache, schema)
}

fn query_and_test(
    cache: &LmdbCache,
    inserted_record: &Record,
    schema_name: &str,
    exp: &QueryExpression,
) -> anyhow::Result<()> {
    let records = cache.query(schema_name, exp)?;
    assert_eq!(records[0], inserted_record.clone(), "must be equal");
    Ok(())
}

#[tokio::test]
async fn insert_and_get_schema() -> anyhow::Result<()> {
    let (cache, schema) = _setup().await;
    cache.insert_schema(&schema, "test")?;
    let schema = cache.get_schema_by_name("test")?;

    let get_schema = cache.get_schema(
        &schema
            .identifier
            .to_owned()
            .context("schema_id is expected")?,
    )?;
    assert_eq!(get_schema, schema, "must be equal");
    Ok(())
}

#[tokio::test]
async fn insert_get_and_delete_record() -> anyhow::Result<()> {
    let val = "bar".to_string();
    let (cache, schema) = _setup().await;
    let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);
    cache.insert_with_schema(&record, &schema, "docs")?;

    let key = helper::get_primary_key(&[0], &[Field::String(val)]);

    let get_record = cache.get(&key)?;
    assert_eq!(get_record, record, "must be equal");

    cache.delete(&key)?;

    cache.get(&key).expect_err("Must not find a record");

    Ok(())
}

#[tokio::test]
async fn insert_and_query_record() -> anyhow::Result<()> {
    let val = "bar".to_string();
    let (cache, schema) = _setup().await;
    let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);

    cache.insert_with_schema(&record, &schema, "docs")?;

    // Query with an expression
    let exp = QueryExpression::new(
        FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Field::String("bar".to_string()),
        ),
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
        &QueryExpression::new(FilterExpression::None, vec![], 10, 0),
    )?;

    Ok(())
}
