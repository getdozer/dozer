use bson::{doc, Document};
use dozer_cache::cache::expression::{FilterExpression, Operator, QueryExpression};
use dozer_types::types::Record;
use futures::stream::StreamExt;
use mongodb::Collection;

use super::film::Film;

/// To validate the filter implementation, we ignore `limit` and `skip` settings,
/// query all the results and compare them with mongodb results, sorted by `film_id`.
pub async fn validate(
    query: &QueryExpression,
    mut records: Vec<Record>,
    collection: &Collection<Film>,
) {
    records.sort_by_key(|record| record.values[0].as_uint().unwrap());

    let filter = convert_filter(query.filter.as_ref());
    let mut cursor = collection.find(filter, None).await.unwrap();

    let mut films = vec![];
    while let Some(film) = cursor.next().await {
        let film = film.unwrap();
        films.push(film);
    }
    films.sort_by_key(|film| film.film_id);

    assert_eq!(records.len(), films.len());
    for (record, film) in records.iter().zip(films.iter()) {
        check_equals(film, record);
    }
}

fn convert_filter(filter: Option<&FilterExpression>) -> Document {
    fn insert_filter_to_document_recursive(document: &mut Document, filter: &FilterExpression) {
        match filter {
            FilterExpression::Simple(name, operator, value) => match operator {
                Operator::LT | Operator::LTE | Operator::EQ | Operator::GT | Operator::GTE => {
                    let operator = match operator {
                        Operator::LT => "$lt",
                        Operator::LTE => "$lte",
                        Operator::EQ => "$eq",
                        Operator::GT => "$gt",
                        Operator::GTE => "$gte",
                        _ => unreachable!(),
                    };
                    document.insert(name, doc! {operator: bson::to_bson(value).unwrap()});
                }
                Operator::Contains => {
                    document.insert(
                        "$text",
                        doc! {
                            "$search": bson::to_bson(value).unwrap()
                        },
                    );
                }
                _ => panic!("Unsupported operator"),
            },
            FilterExpression::And(filters) => {
                for filter in filters {
                    insert_filter_to_document_recursive(document, filter)
                }
            }
        }
    }

    let mut document = Document::new();
    if let Some(filter) = filter {
        insert_filter_to_document_recursive(&mut document, filter);
    }
    document
}

fn check_equals(film: &Film, record: &Record) {
    let mut values = record.values.iter();
    assert_eq!(film.film_id, values.next().unwrap().as_uint().unwrap());
    assert_eq!(film.title, values.next().unwrap().as_string().unwrap());
    assert_eq!(
        film.description,
        values.next().unwrap().as_string().unwrap()
    );
    assert_eq!(film.release_year, values.next().unwrap().as_uint().unwrap());
    assert_eq!(film.language_id, values.next().unwrap().as_uint().unwrap());
    assert_eq!(film.original_language_id, values.next().unwrap().as_uint());
    assert_eq!(
        film.rental_duration,
        values.next().unwrap().as_uint().unwrap()
    );
    assert_eq!(film.rental_rate, values.next().unwrap().as_float().unwrap());
    assert_eq!(film.length, values.next().unwrap().as_uint().unwrap());
    assert_eq!(
        film.replacement_cost,
        values.next().unwrap().as_float().unwrap()
    );
    assert_eq!(film.rating, values.next().unwrap().as_string().unwrap());
    assert_eq!(
        film.last_update,
        values.next().unwrap().as_timestamp().unwrap()
    );
    assert_eq!(
        film.special_features,
        values.next().unwrap().as_string().unwrap()
    );
    assert!(values.next().is_none());
}
