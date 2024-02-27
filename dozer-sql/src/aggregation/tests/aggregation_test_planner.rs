use crate::planner::projection::CommonPlanner;
use crate::tests::utils::get_select;
use crate::{aggregation::processor::AggregationProcessor, tests::utils::create_test_runtime};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
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

    let runtime = create_test_runtime();
    let mut projection_planner = CommonPlanner::new(schema.clone(), &[], runtime.clone());
    let statement = get_select(sql).unwrap();

    runtime
        .block_on(projection_planner.plan(
            statement.projection,
            statement.group_by,
            statement.having,
        ))
        .unwrap();

    let mut processor = AggregationProcessor::new(
        "".to_string(),
        projection_planner.groupby,
        projection_planner.aggregation_output,
        projection_planner.projection_output,
        projection_planner.having,
        schema,
        projection_planner.post_aggregation_schema,
        false,
        None,
    )
    .unwrap();

    let rec = Record::new(vec![
        Field::String("John Smith".to_string()),
        Field::String("Johor".to_string()),
        Field::String("Malaysia".to_string()),
        Field::Int(2),
        Field::Int(1),
    ]);
    let _r = processor.aggregate(Operation::Insert { new: rec }).unwrap();

    let rec = Record::new(vec![
        Field::String("Todd Enton".to_string()),
        Field::String("Johor".to_string()),
        Field::String("Malaysia".to_string()),
        Field::Int(2),
        Field::Int(1),
    ]);
    let _r = processor.aggregate(Operation::Insert { new: rec }).unwrap();
}
