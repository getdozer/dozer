use crate::pipeline::aggregation::processor_new::AggregationProcessor;
use crate::pipeline::planner::projection::ProjectionPlanner;
use crate::pipeline::tests::utils::get_select;
use dozer_core::dag::node::Processor;
use dozer_core::storage::lmdb_storage::LmdbEnvironmentManager;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};
use std::path::Path;
use tempdir::TempDir;

#[test]
fn test_planner_with_aggregator() {
    let sql = "SELECT city, SUM(household_count) as headcounts GROUP BY city";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "household_name".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Table {
                    name: "households".to_string(),
                    connection: "test".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "city".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Table {
                    name: "households".to_string(),
                    connection: "test".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "household_count".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    name: "households".to_string(),
                    connection: "test".to_string(),
                },
            ),
            false,
        )
        .clone();

    let mut projection_planner = ProjectionPlanner::new(schema.clone());
    let statement = get_select(sql).unwrap();

    projection_planner.plan(*statement).unwrap();

    let mut processor = AggregationProcessor::new(
        projection_planner.groupby,
        projection_planner.aggregation_output,
        schema.clone(),
    )
    .unwrap();

    let mut storage =
        LmdbEnvironmentManager::create(TempDir::new("dozer").unwrap().path(), "aggregation_test")
            .unwrap();

    processor.init(&mut storage).unwrap();
    let mut tx = storage.create_txn().unwrap();

    let r = processor
        .aggregate(
            &mut tx.write(),
            processor.db.unwrap(),
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::String("John Smith".to_string()),
                        Field::String("Singapore".to_string()),
                        Field::Int(3),
                    ],
                    None,
                ),
            },
        )
        .unwrap();

    let r = processor
        .aggregate(
            &mut tx.write(),
            processor.db.unwrap(),
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::String("Todd Enton".to_string()),
                        Field::String("Singapore".to_string()),
                        Field::Int(2),
                    ],
                    None,
                ),
            },
        )
        .unwrap();

    println!("aaa");
}
