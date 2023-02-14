use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use sqlparser::ast::SelectItem;
use tempdir::TempDir;
use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_types::log::debug;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
use crate::pipeline::builder::statement_to_pipeline;
use crate::pipeline::expression::builder::{ExpressionBuilder, ExpressionContext};
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::product::tests::pipeline_test::{TestSinkFactory, TestSourceFactory};
use crate::pipeline::tests::utils::get_set_operation;

#[test]
fn test_union_pipeline_builder() {
    dozer_tracing::init_telemetry(false).unwrap();

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        "SELECT supplier_id
        FROM suppliers
        UNION
        SELECT supplier_id
        FROM orders;",
        &mut pipeline, Some("results".to_string())
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();
    let latch = Arc::new(AtomicBool::new(true));
    //
    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "conn".to_string(),
        Arc::new(TestSourceFactory::new(latch.clone())),
        vec![
            ("suppliers".to_string(), 1),
            ("orders".to_string(), 2),
        ]
        .into_iter()
        .collect(),
    ))
    .unwrap();

    pipeline.add_sink(Arc::new(TestSinkFactory::new(8, latch)), "sink");
    pipeline
        .connect_nodes(
            &table_info.node,
            Some(table_info.port),
            "sink",
            Some(DEFAULT_PORT_HANDLE),
            true,
        )
        .unwrap();
    let mut app = App::new(asm);
    app.add_pipeline(pipeline);

    let dag = app.get_dag().unwrap();

    dag.print_dot();

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        std::fs::remove_dir_all(tmp_dir.path())
            .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    std::fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    use std::time::Instant;
    let now = Instant::now();

    let tmp_dir = TempDir::new("test").unwrap();

    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}

#[test]
fn test_union_select() {
    let sql = "
        SELECT supplier_id
        FROM suppliers
        UNION
        SELECT supplier_id
        FROM orders;
        ";

    let left_schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "supplier_id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    connection: "connection1".to_string(),
                    name: "suppliers".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "supplier_name".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Table {
                    connection: "connection1".to_string(),
                    name: "suppliers".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let right_schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "order_id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    connection: "connection1".to_string(),
                    name: "orders".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "order_date".to_string(),
                FieldType::Date,
                false,
                SourceDefinition::Table {
                    connection: "connection1".to_string(),
                    name: "orders".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "supplier_id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    connection: "connection1".to_string(),
                    name: "orders".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let set_operation = get_set_operation(sql).unwrap();

    let left_projection = set_operation.left.projection;
    let mut context = ExpressionContext::new(left_schema.fields.len());
    let mut e = match &left_projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, e, &left_schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            offset: left_schema.fields.len(),
            aggregations: vec![]
        }
    );
    assert_eq!(
        e,
        Box::new(Expression::Column { index: 0 }) // supplier_id from suppliers
    );

    let right_projection = set_operation.right.projection;
    context = ExpressionContext::new(right_schema.fields.len());
    e = match &right_projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, e, &right_schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            offset: right_schema.fields.len(),
            aggregations: vec![]
        }
    );
    assert_eq!(
        e,
        Box::new(Expression::Column { index: 2 }) // supplier_id from orders
    );
}
