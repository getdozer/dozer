use crate::tests::utils::get_select;
use crate::{planner::projection::CommonPlanner, tests::utils::create_test_runtime};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_schema_index_partial_group_by() {
    let sql = "SELECT COUNT(a), b FROM t0 GROUP BY b, c";
    let schema = Schema::default()
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
        .field(
            FieldDefinition::new(
                "c".to_string(),
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

    let runtime = create_test_runtime();
    let mut projection_planner = CommonPlanner::new(schema, &[], runtime.clone());
    let statement = get_select(sql).unwrap();

    runtime
        .block_on(projection_planner.plan(*statement))
        .unwrap();

    assert!(projection_planner
        .post_projection_schema
        .primary_index
        .is_empty(),)
}

#[test]
fn test_schema_index_full_group_by() {
    let sql = "SELECT COUNT(a), c, b  FROM t0 GROUP BY b, c";
    let schema = Schema::default()
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
        .field(
            FieldDefinition::new(
                "c".to_string(),
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

    let runtime = create_test_runtime();
    let mut projection_planner = CommonPlanner::new(schema, &[], runtime.clone());
    let statement = get_select(sql).unwrap();

    runtime
        .block_on(projection_planner.plan(*statement))
        .unwrap();

    assert_eq!(
        projection_planner.post_projection_schema.primary_index,
        vec![1, 2]
    );
}
