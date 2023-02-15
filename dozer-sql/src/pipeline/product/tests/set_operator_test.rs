// use std::sync::Arc;
// use std::sync::atomic::AtomicBool;
// use sqlparser::ast::SelectItem;
// use tempdir::TempDir;
// use dozer_core::app::{App, AppPipeline};
// use dozer_core::appsource::{AppSource, AppSourceManager};
// use dozer_core::DEFAULT_PORT_HANDLE;
// use dozer_core::executor::{DagExecutor, ExecutorOptions};
// use dozer_types::log::debug;
// use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
// use crate::pipeline::builder::statement_to_pipeline;
// use crate::pipeline::expression::builder::{ExpressionBuilder, ExpressionContext};
// use crate::pipeline::expression::execution::Expression;
// use crate::pipeline::product::tests::pipeline_test::{TestSinkFactory, TestSourceFactory};
// use crate::pipeline::tests::utils::get_set_operation;
//

use std::sync::Arc;
use sqlparser::ast::Statement;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use dozer_core::app::AppPipeline;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_core::node::PortHandle;
use dozer_types::log::info;
use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::builder::{get_entry_points, get_input_names, get_input_tables, QueryContext, QueryTableInfo, statement_to_pipeline, TableInfo};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder::NameOrAlias;
use crate::pipeline::product::factory::FromProcessorFactory;
use crate::pipeline::selection::factory::SelectionProcessorFactory;
use crate::pipeline::tests::utils::get_set_operation;

#[test]
fn test_set_union() {
    let sql = "SELECT supplier_id
        FROM suppliers
        UNION
        SELECT supplier_id
        FROM orders;";

    let dialect = AnsiDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    // let query_name = NameOrAlias(format!("query_{}", uuid::Uuid::new_v4()), None);

    let set_operation = get_set_operation(sql).unwrap_or_else(|e| panic!("{}", e.to_string()));

    let left_select = set_operation.left;
    let right_select = set_operation.right;

    let mut pipeline = AppPipeline::new();
    let mut query_ctx = QueryContext::default();
    // let mut query_ctx = statement_to_pipeline(sql, &mut pipeline, Some("results".to_string())).unwrap();

    let mut output_keys = query_ctx.output_tables_map.keys().collect::<Vec<_>>();

    for (idx, statement) in ast.iter().enumerate() {
        let left_input_tables = get_input_tables(&left_select.from[0], &mut pipeline, &mut query_ctx, idx).unwrap();
        let right_input_tables = get_input_tables(&right_select.from[0], &mut pipeline, &mut query_ctx, idx).unwrap();
        info!("{}", statement);

        let left_factory = FromProcessorFactory::new(left_input_tables.clone());
        let right_factory = FromProcessorFactory::new(right_input_tables.clone());

        let left_input_endpoints = get_entry_points(&left_input_tables, &mut query_ctx.pipeline_map, idx).unwrap();
        let right_input_endpoints = get_entry_points(&right_input_tables, &mut query_ctx.pipeline_map, idx).unwrap();

        let gen_product_name = format!("product_{}", uuid::Uuid::new_v4());
        let gen_agg_name = format!("agg_{}", uuid::Uuid::new_v4());
        let gen_selection_name = format!("select_{}", uuid::Uuid::new_v4());
        pipeline.add_processor(Arc::new(left_factory), &gen_product_name, left_input_endpoints);
        pipeline.add_processor(Arc::new(right_factory), &gen_product_name, right_input_endpoints);

        let mut input_names: Vec<NameOrAlias> = Vec::new();
        get_input_names(&left_input_tables).iter().for_each(|name| input_names.push(name.clone()));
        get_input_names(&right_input_tables).iter().for_each(|name| input_names.push(name.clone()));
        for (port_index, table_name) in input_names.iter().enumerate() {
            if let Some(table_info) = query_ctx
                .pipeline_map
                .get(&(idx, table_name.0.clone()))
            {
                pipeline.connect_nodes(
                    &table_info.node,
                    Some(table_info.port),
                    &gen_product_name,
                    Some(port_index as PortHandle),
                    true,
                ).unwrap();
                // If not present in pipeline_map, insert into used_sources as this is coming from source
            } else {
                query_ctx.used_sources.push(table_name.0.clone());
            }
        }

        let left_aggregation = AggregationProcessorFactory::new(left_select.clone(), true);
        let right_aggregation = AggregationProcessorFactory::new(right_select.clone(), true);

        pipeline.add_processor(Arc::new(left_aggregation), &gen_agg_name, vec![]);
        pipeline.add_processor(Arc::new(right_aggregation), &gen_agg_name, vec![]);

        // Where clause
        if let Some(selection) = left_select.clone().selection {
            let selection = SelectionProcessorFactory::new(selection);
            pipeline.add_processor(Arc::new(selection), &gen_selection_name, vec![]);

            pipeline.connect_nodes(
                &gen_product_name,
                Some(DEFAULT_PORT_HANDLE),
                &gen_selection_name,
                Some(DEFAULT_PORT_HANDLE),
                true,
            ).unwrap();

            pipeline.connect_nodes(
                &gen_selection_name,
                Some(DEFAULT_PORT_HANDLE),
                &gen_agg_name,
                Some(DEFAULT_PORT_HANDLE),
                true,
            ).unwrap();
        } else {
            pipeline.connect_nodes(
                &gen_product_name,
                Some(DEFAULT_PORT_HANDLE),
                &gen_agg_name,
                Some(DEFAULT_PORT_HANDLE),
                true,
            ).unwrap();
        }

        if let Some(selection) = right_select.clone().selection {
            let selection = SelectionProcessorFactory::new(selection);
            pipeline.add_processor(Arc::new(selection), &gen_selection_name, vec![]);

            pipeline.connect_nodes(
                &gen_product_name,
                Some(DEFAULT_PORT_HANDLE),
                &gen_selection_name,
                Some(DEFAULT_PORT_HANDLE),
                true,
            ).unwrap();

            pipeline.connect_nodes(
                &gen_selection_name,
                Some(DEFAULT_PORT_HANDLE),
                &gen_agg_name,
                Some(DEFAULT_PORT_HANDLE),
                true,
            ).unwrap();
        } else {
            pipeline.connect_nodes(
                &gen_product_name,
                Some(DEFAULT_PORT_HANDLE),
                &gen_agg_name,
                Some(DEFAULT_PORT_HANDLE),
                true,
            ).unwrap();
        }

        let table_name = "results".to_string();
        query_ctx.pipeline_map.insert(
            (idx, table_name.clone()),
            QueryTableInfo {
                node: gen_agg_name.clone(),
                port: DEFAULT_PORT_HANDLE,
                is_derived: false,
            },
        );

        let table_name = query_ctx.output_tables_map.insert(
            table_name,
            QueryTableInfo {
                node: gen_agg_name,
                port: DEFAULT_PORT_HANDLE,
                is_derived: false,
            },
        );
    }

    info!("{:?}", query_ctx.pipeline_map);


}

// #[test]
// fn test_union_pipeline_builder() {
//     dozer_tracing::init_telemetry(false).unwrap();
//
//     let mut pipeline = AppPipeline::new();
//
//     let context = statement_to_pipeline(
//         "SELECT supplier_id
//         FROM suppliers
//         UNION
//         SELECT supplier_id
//         FROM orders;",
//         &mut pipeline, Some("results".to_string())
//     )
//     .unwrap();
//
//     let table_info = context.output_tables_map.get("results").unwrap();
//     let latch = Arc::new(AtomicBool::new(true));
//     //
//     let mut asm = AppSourceManager::new();
//     asm.add(AppSource::new(
//         "conn".to_string(),
//         Arc::new(TestSourceFactory::new(latch.clone())),
//         vec![
//             ("suppliers".to_string(), 1),
//             ("orders".to_string(), 2),
//         ]
//         .into_iter()
//         .collect(),
//     ))
//     .unwrap();
//
//     pipeline.add_sink(Arc::new(TestSinkFactory::new(8, latch)), "sink");
//     pipeline
//         .connect_nodes(
//             &table_info.node,
//             Some(table_info.port),
//             "sink",
//             Some(DEFAULT_PORT_HANDLE),
//             true,
//         )
//         .unwrap();
//     let mut app = App::new(asm);
//     app.add_pipeline(pipeline);
//
//     let dag = app.get_dag().unwrap();
//
//     dag.print_dot();
//
//     let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
//     if tmp_dir.path().exists() {
//         std::fs::remove_dir_all(tmp_dir.path())
//             .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
//     }
//     std::fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));
//
//     use std::time::Instant;
//     let now = Instant::now();
//
//     let tmp_dir = TempDir::new("test").unwrap();
//
//     DagExecutor::new(
//         &dag,
//         tmp_dir.path().to_path_buf(),
//         ExecutorOptions::default(),
//     )
//         .unwrap()
//         .start(Arc::new(AtomicBool::new(true)))
//         .unwrap()
//         .join()
//         .unwrap();
//
//     let elapsed = now.elapsed();
//     debug!("Elapsed: {:.2?}", elapsed);
// }
//
// #[test]
// fn test_union_select() {
//     let sql = "
//         SELECT supplier_id
//         FROM suppliers
//         UNION
//         SELECT supplier_id
//         FROM orders;
//         ";
//
//     let left_schema = Schema::empty()
//         .field(
//             FieldDefinition::new(
//                 "supplier_id".to_string(),
//                 FieldType::Int,
//                 false,
//                 SourceDefinition::Table {
//                     connection: "connection1".to_string(),
//                     name: "suppliers".to_string(),
//                 },
//             ),
//             false,
//         )
//         .field(
//             FieldDefinition::new(
//                 "supplier_name".to_string(),
//                 FieldType::String,
//                 false,
//                 SourceDefinition::Table {
//                     connection: "connection1".to_string(),
//                     name: "suppliers".to_string(),
//                 },
//             ),
//             false,
//         )
//         .to_owned();
//
//     let right_schema = Schema::empty()
//         .field(
//             FieldDefinition::new(
//                 "order_id".to_string(),
//                 FieldType::Int,
//                 false,
//                 SourceDefinition::Table {
//                     connection: "connection1".to_string(),
//                     name: "orders".to_string(),
//                 },
//             ),
//             false,
//         )
//         .field(
//             FieldDefinition::new(
//                 "order_date".to_string(),
//                 FieldType::Date,
//                 false,
//                 SourceDefinition::Table {
//                     connection: "connection1".to_string(),
//                     name: "orders".to_string(),
//                 },
//             ),
//             false,
//         )
//         .field(
//             FieldDefinition::new(
//                 "supplier_id".to_string(),
//                 FieldType::Int,
//                 false,
//                 SourceDefinition::Table {
//                     connection: "connection1".to_string(),
//                     name: "orders".to_string(),
//                 },
//             ),
//             false,
//         )
//         .to_owned();
//
//     let set_operation = get_set_operation(sql).unwrap();
//
//     let left_projection = set_operation.left.projection;
//     let mut context = ExpressionContext::new(left_schema.fields.len());
//     let mut e = match &left_projection[0] {
//         SelectItem::UnnamedExpr(e) => {
//             ExpressionBuilder::build(&mut context, true, e, &left_schema).unwrap()
//         }
//         _ => panic!("Invalid expr"),
//     };
//
//     assert_eq!(
//         context,
//         ExpressionContext {
//             offset: left_schema.fields.len(),
//             aggregations: vec![]
//         }
//     );
//     assert_eq!(
//         e,
//         Box::new(Expression::Column { index: 0 }) // supplier_id from suppliers
//     );
//
//     let right_projection = set_operation.right.projection;
//     context = ExpressionContext::new(right_schema.fields.len());
//     e = match &right_projection[0] {
//         SelectItem::UnnamedExpr(e) => {
//             ExpressionBuilder::build(&mut context, true, e, &right_schema).unwrap()
//         }
//         _ => panic!("Invalid expr"),
//     };
//
//     assert_eq!(
//         context,
//         ExpressionContext {
//             offset: right_schema.fields.len(),
//             aggregations: vec![]
//         }
//     );
//     assert_eq!(
//         e,
//         Box::new(Expression::Column { index: 2 }) // supplier_id from orders
//     );
// }
