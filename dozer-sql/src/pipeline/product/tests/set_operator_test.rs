use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use sqlparser::dialect::AnsiDialect;
use sqlparser::keywords::Keyword::ORDER;
use sqlparser::parser::Parser;
use tempdir::TempDir;
use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, ProcessorFactory, Sink, SinkFactory, Source, SourceFactory};
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_types::chrono::NaiveDate;
use dozer_types::log::{debug, info};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition};
use crate::pipeline::builder::{get_input_tables, QueryContext, SchemaSQLContext, statement_to_pipeline};
use crate::pipeline::product::set_factory::SetProcessorFactory;
use crate::pipeline::projection::factory::ProjectionProcessorFactory;
use crate::pipeline::tests::utils::get_set_operation;

#[test]
fn test_set_union_pipeline_builder() {
    let sql = "WITH supplier_id_union AS (
                        SELECT supplier_id
                        FROM suppliers
                        UNION
                        SELECT supplier_id
                        FROM orders
                    )
                    SELECT supplier_id_union.supplier_id
                    INTO set_results
                    FROM supplier_id_union;";

    dozer_tracing::init_telemetry(false).unwrap();

    let mut pipeline: AppPipeline<SchemaSQLContext> = AppPipeline::new();
    let mut query_ctx = statement_to_pipeline(sql, &mut pipeline, Some("set_results".to_string())).unwrap();

    let table_info = query_ctx.output_tables_map.get("set_results").unwrap();
    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "conn".to_string(),
        Arc::new(TestSourceFactory::new(latch.clone())),
        vec![
            ("suppliers".to_string(), SUPPLIERS_PORT),
            ("orders".to_string(), ORDERS_PORT),
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
    info!("Elapsed: {:.2?}", elapsed);
}

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

    let suppliers_schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "supplier_id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    name: "conn".to_string(),
                    connection: "suppliers".to_string(),
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
                    name: "conn".to_string(),
                    connection: "suppliers".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let orders_schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "order_id".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    name: "conn".to_string(),
                    connection: "orders".to_string(),
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
                    name: "conn".to_string(),
                    connection: "orders".to_string(),
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
                    name: "conn".to_string(),
                    connection: "orders".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let mut pipeline = AppPipeline::new();
    let mut query_ctx = QueryContext::default();
    // let mut query_ctx = statement_to_pipeline(sql, &mut pipeline, Some("results".to_string())).unwrap();

    let mut output_keys = query_ctx.output_tables_map.keys().collect::<Vec<_>>();

    for (idx, statement) in ast.iter().enumerate() {
        let left_input_tables = get_input_tables(&left_select.clone().from[0], &mut pipeline, &mut query_ctx, idx).unwrap();
        let right_input_tables = get_input_tables(&right_select.clone().from[0], &mut pipeline, &mut query_ctx, idx).unwrap();

        let set_factory = SetProcessorFactory::new(left_input_tables.clone(), right_input_tables.clone());
        let input_ports = set_factory.get_input_ports();

        let left_factory = ProjectionProcessorFactory::new(left_select.clone().projection);
        let left_output_schema = left_factory.get_output_schema(
            input_ports.first().unwrap(),
            &[
                (DEFAULT_PORT_HANDLE, (suppliers_schema.clone(), SchemaSQLContext::default())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();

        let right_factory = ProjectionProcessorFactory::new(right_select.clone().projection);
        let right_output_schema = right_factory.get_output_schema(
            input_ports.first().unwrap(),
            &[
                (DEFAULT_PORT_HANDLE, (orders_schema.clone(), SchemaSQLContext::default())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();

        assert_eq!(left_output_schema.0, right_output_schema.0);

        set_factory.get_output_ports();
        let output_schema = set_factory.get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[
                (*input_ports.first().unwrap(), left_output_schema),
                (*input_ports.last().unwrap(), right_output_schema),
            ]
            .into_iter()
            .collect(),
        ).unwrap();

        println!("{:?}", output_schema);
    }

    //     let left_input_endpoints = get_entry_points(&left_input_tables, &mut query_ctx.pipeline_map, idx).unwrap();
    //     let right_input_endpoints = get_entry_points(&right_input_tables, &mut query_ctx.pipeline_map, idx).unwrap();
    //
    //     let gen_product_name = format!("product_{}", uuid::Uuid::new_v4());
    //     let gen_agg_name = format!("agg_{}", uuid::Uuid::new_v4());
    //     let gen_selection_name = format!("select_{}", uuid::Uuid::new_v4());
    //     pipeline.add_processor(Arc::new(left_factory), &gen_product_name, left_input_endpoints);
    //     pipeline.add_processor(Arc::new(right_factory), &gen_product_name, right_input_endpoints);
    //
    //     let mut input_names: Vec<NameOrAlias> = Vec::new();
    //     get_input_names(&left_input_tables).iter().for_each(|name| input_names.push(name.clone()));
    //     get_input_names(&right_input_tables).iter().for_each(|name| input_names.push(name.clone()));
    //     for (port_index, table_name) in input_names.iter().enumerate() {
    //         if let Some(table_info) = query_ctx
    //             .pipeline_map
    //             .get(&(idx, table_name.0.clone()))
    //         {
    //             pipeline.connect_nodes(
    //                 &table_info.node,
    //                 Some(table_info.port),
    //                 &gen_product_name,
    //                 Some(port_index as PortHandle),
    //                 true,
    //             ).unwrap();
    //             // If not present in pipeline_map, insert into used_sources as this is coming from source
    //         } else {
    //             query_ctx.used_sources.push(table_name.0.clone());
    //         }
    //     }
    //
    //     let left_aggregation = AggregationProcessorFactory::new(left_select.clone(), true);
    //     let right_aggregation = AggregationProcessorFactory::new(right_select.clone(), true);
    //
    //     pipeline.add_processor(Arc::new(left_aggregation), &gen_agg_name, vec![]);
    //     pipeline.add_processor(Arc::new(right_aggregation), &gen_agg_name, vec![]);
    //
    //     // Where clause
    //     if let Some(selection) = left_select.clone().selection {
    //         let selection = SelectionProcessorFactory::new(selection);
    //         pipeline.add_processor(Arc::new(selection), &gen_selection_name, vec![]);
    //
    //         pipeline.connect_nodes(
    //             &gen_product_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             &gen_selection_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             true,
    //         ).unwrap();
    //
    //         pipeline.connect_nodes(
    //             &gen_selection_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             &gen_agg_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             true,
    //         ).unwrap();
    //     } else {
    //         pipeline.connect_nodes(
    //             &gen_product_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             &gen_agg_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             true,
    //         ).unwrap();
    //     }
    //
    //     if let Some(selection) = right_select.clone().selection {
    //         let selection = SelectionProcessorFactory::new(selection);
    //         pipeline.add_processor(Arc::new(selection), &gen_selection_name, vec![]);
    //
    //         pipeline.connect_nodes(
    //             &gen_product_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             &gen_selection_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             true,
    //         ).unwrap();
    //
    //         pipeline.connect_nodes(
    //             &gen_selection_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             &gen_agg_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             true,
    //         ).unwrap();
    //     } else {
    //         pipeline.connect_nodes(
    //             &gen_product_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             &gen_agg_name,
    //             Some(DEFAULT_PORT_HANDLE),
    //             true,
    //         ).unwrap();
    //     }
    //
    //     let table_name = "results".to_string();
    //     query_ctx.pipeline_map.insert(
    //         (idx, table_name.clone()),
    //         QueryTableInfo {
    //             node: gen_agg_name.clone(),
    //             port: DEFAULT_PORT_HANDLE,
    //             is_derived: false,
    //         },
    //     );
    //
    //     let table_name = query_ctx.output_tables_map.insert(
    //         table_name,
    //         QueryTableInfo {
    //             node: gen_agg_name,
    //             port: DEFAULT_PORT_HANDLE,
    //             is_derived: false,
    //         },
    //     );
    // }
    //
    // info!("{:?}", query_ctx.pipeline_map);


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

// #[test]
// fn test_set_union_pipeline() {
//     let sql = "SELECT supplier_id
//                     FROM suppliers
//                     JOIN orders
//                     ON suppliers.supplier_id = orders.supplier_id;";
//
//     dozer_tracing::init_telemetry(false).unwrap();
//
//     let mut pipeline: AppPipeline<SchemaSQLContext> = AppPipeline::new();
//     let mut query_ctx = statement_to_pipeline(sql, &mut pipeline, Some("set_results".to_string())).unwrap();
//
//     let table_info = query_ctx.output_tables_map.get("set_results").unwrap();
//     let latch = Arc::new(AtomicBool::new(true));
//
//     const SUPPLIERS_PORT: u16 = 0 as PortHandle;
//     const ORDERS_PORT: u16 = 1 as PortHandle;
//
//     let mut asm = AppSourceManager::new();
//     asm.add(AppSource::new(
//         "conn".to_string(),
//         Arc::new(TestSourceFactory::new(latch.clone())),
//         vec![
//             ("suppliers".to_string(), SUPPLIERS_PORT),
//             ("orders".to_string(), ORDERS_PORT),
//         ]
//             .into_iter()
//             .collect(),
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
//
//     let mut app = App::new(asm);
//     app.add_pipeline(pipeline);
//
//     let dag = app.get_dag().unwrap();
// }

const SUPPLIERS_PORT: u16 = 0 as PortHandle;
const ORDERS_PORT: u16 = 1 as PortHandle;

#[derive(Debug)]
pub struct TestSourceFactory {
    running: Arc<AtomicBool>,
}

impl TestSourceFactory {
    pub fn new(running: Arc<AtomicBool>) -> Self {
        Self { running }
    }
}

impl SourceFactory<SchemaSQLContext> for TestSourceFactory {
    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(vec![
            OutputPortDef::new(
                SUPPLIERS_PORT,
                OutputPortType::Stateless,
            ),
            OutputPortDef::new(
                ORDERS_PORT,
                OutputPortType::Stateless,
            ),
        ])
    }

    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        if port == &SUPPLIERS_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "suppliers".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("supplier_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("supplier_name"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else if port == &ORDERS_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "orders".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("order_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("order_date"),
                            FieldType::Date,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("supplier_id"),
                            FieldType::Int,
                            false,
                            source_id,
                        ),
                        false,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else if port == &DEFAULT_PORT_HANDLE {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "set_result".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("supplier_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else {
            panic!("Invalid Port Handle {port}");
        }
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(TestSource {
            running: self.running.clone(),
        }))
    }

    fn prepare(
        &self,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSource {
    running: Arc<AtomicBool>,
}

impl Source for TestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let operations = vec![
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(1000),
                            Field::String("Microsoft".to_string()),
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(2000),
                            Field::String("Oracle".to_string()),
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(3000),
                            Field::String("Apple".to_string()),
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(4000),
                            Field::String("Samsung".to_string()),
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(1),
                            Field::Date(NaiveDate::from_ymd_opt(2015, 8, 1).unwrap()),
                            Field::Int(2000)
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(2),
                            Field::Date(NaiveDate::from_ymd_opt(2015, 8, 1).unwrap()),
                            Field::Int(6000)
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(3),
                            Field::Date(NaiveDate::from_ymd_opt(2015, 8, 2).unwrap()),
                            Field::Int(7000)
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(4),
                            Field::Date(NaiveDate::from_ymd_opt(2015, 8, 3).unwrap()),
                            Field::Int(8000)
                        ],
                        Some(1),
                    ),
                },
                DEFAULT_PORT_HANDLE,
            ),
        ];

        for operation in operations.iter().enumerate() {
            fw.send(
                operation.0.try_into().unwrap(),
                0,
                operation.1.clone().0,
                operation.1.clone().1,
            )
            .unwrap();
        }

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            // thread::sleep(Duration::from_millis(500));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TestSinkFactory {
    expected: u64,
    running: Arc<AtomicBool>,
}

impl TestSinkFactory {
    pub fn new(expected: u64, barrier: Arc<AtomicBool>) -> Self {
        Self {
            expected,
            running: barrier,
        }
    }
}

impl SinkFactory<SchemaSQLContext> for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(TestSink {
            expected: self.expected,
            current: 0,
            running: self.running.clone(),
        }))
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSink {
    expected: u64,
    current: u64,
    running: Arc<AtomicBool>,
}

impl Sink for TestSink {
    fn init(&mut self, _env: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        match _op {
            Operation::Delete { old } => info!("o0:-> - {:?}", old.values),
            Operation::Insert { new } => info!("o0:-> + {:?}", new.values),
            Operation::Update { old, new } => {
                info!("o0:-> - {:?}, + {:?}", old.values, new.values)
            }
        }

        self.current += 1;
        if self.current == self.expected {
            debug!(
                "Received {} messages. Notifying sender to exit!",
                self.current
            );
            self.running.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    fn commit(&mut self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }
}