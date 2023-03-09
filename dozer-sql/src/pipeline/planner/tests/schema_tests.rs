use crate::pipeline::planner::projection::CommonPlanner;
use crate::pipeline::tests::utils::get_select;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_basic_projection() {
    let sql = "SELECT COUNT(a), b FROM t0 GROUP BY b";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "a".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    name: "t0".to_string(),
                    connection: "c0".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "b".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    name: "t0".to_string(),
                    connection: "c0".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let mut projection_planner = CommonPlanner::new(schema);
    let statement = get_select(sql).unwrap();

    projection_planner.plan(*statement).unwrap();

    assert_eq!(
        projection_planner.post_projection_schema.primary_index,
        vec![1]
    )
}
