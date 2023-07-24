use crate::pipeline::aggregation::processor::AggregationProcessor;
use crate::pipeline::planner::projection::CommonPlanner;
use crate::pipeline::tests::utils::get_select;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, ProcessorRecord, Schema, SourceDefinition,
};

#[test]
fn test_planner_with_aggregator() {
    let sql = "SELECT CONCAT(city,'/',country), CONCAT('Total: ', CAST(SUM(adults_count + children_count) AS STRING), ' people') as headcounts GROUP BY CONCAT(city,'/',country)";
    let schema = Schema::default()
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
        "".to_string(),
        projection_planner.groupby,
        projection_planner.aggregation_output,
        projection_planner.projection_output,
        projection_planner.having,
        schema,
        projection_planner.post_aggregation_schema,
    )
    .unwrap();

    let _r = processor
        .aggregate(Operation::Insert {
            new: ProcessorRecord::new(vec![
                Field::String("John Smith".to_string()),
                Field::String("Johor".to_string()),
                Field::String("Malaysia".to_string()),
                Field::Int(2),
                Field::Int(1),
            ]),
        })
        .unwrap();

    let _r = processor
        .aggregate(Operation::Insert {
            new: ProcessorRecord::new(vec![
                Field::String("Todd Enton".to_string()),
                Field::String("Johor".to_string()),
                Field::String("Malaysia".to_string()),
                Field::Int(2),
                Field::Int(2),
            ]),
        })
        .unwrap();
}
