use dozer_cache::cache::{expression::QueryExpression, Cache, LmdbCache};
use dozer_types::types::Record;

/// Validate if `query.skip` and `query.limit` works correctly by comparing the results
/// with the results of the same query with `skip` 0 and `limit` 1000.
///
/// Returns the query with `skip` 0 and `limit` 1000 and its results.
pub fn validate(
    cache: &LmdbCache,
    schema_name: &str,
    mut query: QueryExpression,
) -> (QueryExpression, Vec<Record>) {
    let records = cache.query(schema_name, &query).unwrap();

    let skip = query.skip;
    let limit = query.limit;

    query.skip = 0;
    query.limit = 1000;
    let all_records = cache.query(schema_name, &query).unwrap();

    let expected = all_records.iter().skip(skip).take(limit);

    assert_eq!(records.len(), expected.len());
    for (record, expected) in records.iter().zip(expected) {
        assert_eq!(record, expected);
    }

    (query, all_records)
}
