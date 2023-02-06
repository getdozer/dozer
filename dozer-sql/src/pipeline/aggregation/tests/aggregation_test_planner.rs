use crate::pipeline::aggregation::processor::AggregationProcessor;
use crate::pipeline::planner::projection::CommonPlanner;
use crate::pipeline::tests::utils::get_select;
use dozer_core::dag::node::Processor;
use dozer_core::storage::lmdb_storage::LmdbEnvironmentManager;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};
use tempdir::TempDir;

#[test]
fn test_planner_with_aggregator() {
    let sql = "SELECT CONCAT(city,'/',country), CONCAT('Total: ', CAST(SUM(adults_count + children_count) AS STRING), ' people') as headcounts GROUP BY CONCAT(city,'/',country)";
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
                "country".to_string(),
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
                "adults_count".to_string(),
                FieldType::Int,
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
                "children_count".to_string(),
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

    let mut projection_planner = CommonPlanner::new(schema.clone());
    let statement = get_select(sql).unwrap();

    projection_planner.plan(*statement).unwrap();

    let mut processor = AggregationProcessor::new(
        projection_planner.groupby,
        projection_planner.aggregation_output,
        projection_planner.projection_output,
        schema.clone(),
        projection_planner.post_aggregation_schema,
    )
    .unwrap();

    let mut storage =
        LmdbEnvironmentManager::create(TempDir::new("dozer").unwrap().path(), "aggregation_test")
            .unwrap();

    processor.init(&mut storage).unwrap();
    let mut tx = storage.create_txn().unwrap();

    let _r = processor
        .aggregate(
            &mut tx.write(),
            processor.db.unwrap(),
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::String("John Smith".to_string()),
                        Field::String("Johor".to_string()),
                        Field::String("Malaysia".to_string()),
                        Field::Int(2),
                        Field::Int(1),
                    ],
                    None,
                ),
            },
        )
        .unwrap();

    let _r = processor
        .aggregate(
            &mut tx.write(),
            processor.db.unwrap(),
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::String("Todd Enton".to_string()),
                        Field::String("Johor".to_string()),
                        Field::String("Malaysia".to_string()),
                        Field::Int(2),
                        Field::Int(2),
                    ],
                    None,
                ),
            },
        )
        .unwrap();
}
