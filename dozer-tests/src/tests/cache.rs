use dozer_cache::cache::{expression::QueryExpression, RoCache};
use dozer_types::{
    serde_json::{self, json, Value},
    types::IndexDefinition,
};
use mongodb::Collection;

use crate::cache_tests::{
    film::{film_schema, load_database, Film},
    filter, order, skip_and_limit,
};

#[tokio::test]
async fn test_cache_query() {
    let secondary_indexes = vec![
        IndexDefinition::SortedInverted(vec![0]),
        IndexDefinition::SortedInverted(vec![3]),
        IndexDefinition::SortedInverted(vec![5]),
        IndexDefinition::SortedInverted(vec![7]),
        IndexDefinition::SortedInverted(vec![3, 0]),
        IndexDefinition::SortedInverted(vec![3, 7, 0]),
        IndexDefinition::SortedInverted(vec![5, 0]),
        IndexDefinition::SortedInverted(vec![7, 0]),
        IndexDefinition::FullText(12),
    ];

    let (cache, schema_name, collection) = load_database(secondary_indexes).await;

    let test_cases = vec![
        // empty
        json!({}),
        // only filter
        json!({ "$filter": { "film_id": 3 } }),
        json!({ "$filter": { "original_language_id": null } }),
        json!({ "$filter": { "rental_rate": 0.99 } }),
        json!({ "$filter": { "film_id": { "$lte": 317 } } }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } } }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 } }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 } }),
        // only order by
        json!({ "$order_by": { "film_id": "desc" } }),
        json!({ "$order_by": { "original_language_id": "asc" } }),
        json!({ "$order_by": { "rental_rate": "asc" } }),
        // only skip
        json!({ "$skip": 17 }),
        json!({ "after": 9 }),
        // only limit
        json!({ "$limit": 21 }),
        // filter + order by
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" } }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" } }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" } }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" } }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" } }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" } }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" } }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" } }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" } }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" } }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" } }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" } }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" } }),
        // filter + skip
        json!({ "$filter": { "release_year": 2006 }, "$skip": 17 }),
        json!({ "$filter": { "release_year": 2006 }, "$skip": 17 }),
        json!({ "$filter": { "original_language_id": null }, "$skip": 17 }),
        json!({ "$filter": { "original_language_id": null }, "$skip": 17 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$skip": 17 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$skip": 17 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$skip": 17 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$skip": 17 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$skip": 17 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$skip": 17 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$skip": 17 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$skip": 17 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$skip": 17 }),
        json!({ "$filter": { "release_year": 2006 }, "$after": 9 }),
        json!({ "$filter": { "release_year": 2006 }, "$after": 9 }),
        json!({ "$filter": { "original_language_id": null }, "$after": 9 }),
        json!({ "$filter": { "original_language_id": null }, "$after": 9 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$after": 9 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$after": 9 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$after": 9 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$after": 9 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$after": 9 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$after": 113 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$after": 113 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$after": 113 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$after": 113 }),
        // filter + limit
        json!({ "$filter": { "release_year": 2006 }, "$limit ": 13 }),
        json!({ "$filter": { "release_year": 2006 }, "$limit ": 13 }),
        json!({ "$filter": { "original_language_id": null }, "$limit ": 13 }),
        json!({ "$filter": { "original_language_id": null }, "$limit ": 13 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$lim": 13 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$limit": 13 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$limit": 13 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$lim": 13 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$lim": 13 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$limit": 13 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$limit": 13 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$limit": 13 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$limit": 13 }),
        // order by + skip
        json!({ "$order_by": { "film_id": "desc" }, "$skip": 21 }),
        json!({ "$order_by": { "original_language_id": "asc" }, "$skip": 21 }),
        json!({ "$order_by": { "rental_rate": "asc" }, "$skip": 21 }),
        json!({ "$order_by": { "film_id": "desc" }, "$after": 11 }),
        json!({ "$order_by": { "original_language_id": "asc" }, "$after": 11 }),
        json!({ "$order_by": { "rental_rate": "asc" }, "$after": 11 }),
        // order by + limit
        json!({ "$order_by": { "film_id": "desc" }, "$limit": 29 }),
        json!({ "$order_by": { "original_language_id": "asc" }, "$limit": 29 }),
        json!({ "$order_by": { "rental_rate": "asc" }, "$limit": 29 }),
        // skip + limit
        json!({ "$skip": 17, "$limit": 11 }),
        json!({ "$after": 19, "$limit": 11 }),
        // filter + order by + skip
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$skip": 7 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$skip": 7 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 7 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$skip": 7 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 7 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$skip": 7 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$after": 117 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$after": 117 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$after": 117 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$after": 117 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$after": 117 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$after": 117 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$after": 117 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$after": 117 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$after": 117 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$after": 117 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$after": 117 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$after": 117 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$after": 117 }),
        // filter + order by + limit
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$limit": 71 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$limit": 71 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$limit": 71 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$limit": 71 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$limit": 71 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$limit": 71 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$limit": 71 }),
        // filter + skip + limit
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$skip": 7, "$limit": 19 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 117, "$limit": 19 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$skip": 117, "$limit": 19 }),
        // order by + skip + limit
        json!({ "$order_by": { "film_id": "desc" }, "$skip": 5, "$limit": 101 }),
        json!({ "$order_by": { "original_language_id": "asc" }, "$skip": 5, "$limit": 101 }),
        json!({ "$order_by": { "rental_rate": "asc" }, "$skip": 5, "$limit": 101 }),
        json!({ "$order_by": { "film_id": "desc" }, "$after": 13, "$limit": 101 }),
        json!({ "$order_by": { "original_language_id": "asc" }, "$after": 13, "$limit": 101 }),
        json!({ "$order_by": { "rental_rate": "asc" }, "$after": 13, "$limit": 101 }),
        // filter + order by + skip + limit
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$skip": 1, "$limit": 199 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "asc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "release_year": 2006 }, "$order_by": { "film_id": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "asc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "original_language_id": null }, "$order_by": { "film_id": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$lt": 317 } }, "$order_by": { "film_id": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "rental_rate": { "$gt": 2 } }, "$order_by": { "rental_rate": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "asc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006 },  "$order_by": { "film_id": "desc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "asc" }, "$after": 112, "$limit": 199 }),
        json!({ "$filter": { "film_id": { "$gte": 113 }, "release_year": 2006, "rental_rate": 0.99 }, "$order_by": { "film_id": "desc" }, "$after": 112, "$limit": 199 }),
        // full text
        json!({ "$filter": { "special_features": { "$contains": "Trailers" } } }),
    ];

    for test_case in test_cases {
        validate_query(&*cache, schema_name, &collection, test_case).await;
    }
}

async fn validate_query(
    cache: &dyn RoCache,
    schema_name: &str,
    collection: &Collection<Film>,
    query: Value,
) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    let (query, records) = skip_and_limit::validate(cache, schema_name, query);
    order::validate(&film_schema(), &records, &query.order_by.0);
    filter::validate(&query, records, collection).await;
}
