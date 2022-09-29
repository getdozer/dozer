use std::rc::Rc;
use std::sync::Arc;
use std::collections::HashMap;
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::comparison::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::pipeline::expression::logical::{And, Not, Or};
use crate::pipeline::expression::mathematical::{Add, Div, Mod, Mul, Sub};
use crate::pipeline::expression::expression::{Column, PhysicalExpression};
use crate::pipeline::processor::selection::{SelectionBuilder, SelectionProcessor};
use crate::pipeline::processor::projection_builder::ProjectionBuilder;
use dozer_core::dag::channel::LocalNodeChannel;
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::dag::{Endpoint, NodeHandle, PortHandle, TestSink, TestSource};
use dozer_core::dag::executor::{MemoryExecutionContext, MultiThreadedDagExecutor};
use dozer_core::dag::node::NextStep::Continue;
use dozer_core::dag::node::{
    ChannelForwarder, ExecutionContext, NextStep, Processor, Sink, Source,
};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema as DozerSchema};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Query, Select, SelectItem, SetExpr, Statement, UnaryOperator,
    Value as SqlValue,
};
use sqlparser::ast::ObjectType::Schema;

pub struct PipelineBuilder {
    schema: DozerSchema,
}

impl PipelineBuilder {
    pub fn new(schema: DozerSchema) -> PipelineBuilder {
        Self {
            schema
        }
    }

    pub fn statement_to_pipeline(&self, statement: Statement) -> Result<(Dag, NodeHandle, NodeHandle)> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    pub fn query_to_pipeline(&self, query: Query) -> Result<(Dag, NodeHandle, NodeHandle)> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(&self, set_expr: SetExpr) -> Result<(Dag, NodeHandle, NodeHandle)> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    fn select_to_pipeline(&self, select: Select) -> Result<(Dag, NodeHandle, NodeHandle)> {

        // Select clause
        let projection_processor = ProjectionBuilder::new(&self.schema).get_processor(select.projection)?;

        // Where clause
        let selection_processor = SelectionBuilder::new(&self.schema).get_processor(select.selection)?;

        let mut dag = Dag::new();

        let projection_handle = dag.add_node(NodeType::Processor(projection_processor));
        let selection_handle = dag.add_node(NodeType::Processor(selection_processor));

        let _ = dag.connect(
            Endpoint::new(projection_handle, None),
            Endpoint::new(selection_handle, None),
            Box::new(LocalNodeChannel::new(5000000))
        );

        Ok((dag, projection_handle, selection_handle))
    }

}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SqlTestSource {
    id: i32,
    output_ports: Option<Vec<PortHandle>>,
}

impl SqlTestSource {
    pub fn new(id: i32, output_ports: Option<Vec<PortHandle>>) -> Self {
        Self { id, output_ports }
    }
}

impl Source for SqlTestSource {
    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> std::result::Result<(), String> {
        println!("SRC {}: Initialising TestProcessor", self.id);
        Ok(())
    }

    fn start(&self, fw: &ChannelForwarder) -> std::result::Result<(), String> {
        for n in 0..10000000 {
            //   println!("SRC {}: Message {} received", self.id, n);
            fw.send(
                OperationEvent::new(
                    n,
                    Operation::Insert {
                        new: Record::new(None, vec![Field::Int(0), Field::String("Italy".to_string()), Field::Int(2000)]),
                    },
                ),
                None,
            );
        }
        fw.terminate();
        Ok(())
    }
}

pub struct SqlTestSink {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
}

impl SqlTestSink {
    pub fn new(id: i32, input_ports: Option<Vec<PortHandle>>) -> Self {
        Self { id, input_ports }
    }
}

impl Sink for SqlTestSink {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn init(&self) -> std::result::Result<(), String> {
        println!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(&self, from_port: Option<PortHandle>, op: OperationEvent, ctx: & dyn ExecutionContext) -> std::result::Result<NextStep, String> {
        // println!("SINK {}: Message {} received", self.id, op.id);
        Ok(Continue)
    }
}

#[test]
fn test_pipeline_builder() {
    let sql = "SELECT 1, 1+1, Country+1, COUNT(Spending), ROUND(SUM(ROUND(Spending))) \
                            FROM Customers \
                            WHERE Spending >= 1000 \
                            GROUP BY Country \
                            HAVING COUNT(CustomerID) > 1;";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("AST: {:?}", ast);


    let statement: &Statement = &ast[0];

    let schema = DozerSchema {
        fields: vec![
            FieldDefinition {name: String::from("CustomerID"), typ: FieldType::Int, nullable: false},
            FieldDefinition {name: String::from("Country"), typ: FieldType::String, nullable: false},
            FieldDefinition {name: String::from("Spending"), typ: FieldType::Int, nullable: false},
        ],
        values: vec![0], primary_index: vec![], secondary_indexes: vec![], identifier: None
    };

    let builder = PipelineBuilder::new(schema);
    let (mut dag, in_handle, out_handle) = builder.statement_to_pipeline(statement.clone()).unwrap();

    let source = SqlTestSource::new(1,None);
    let sink = SqlTestSink::new(1, None);

    let src_handle = dag.add_node(NodeType::Source(Arc::new(source)));
    let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));

    let src_to_proc1 = dag.connect(
        Endpoint::new(src_handle, None),
        Endpoint::new(in_handle, None),
        Box::new(LocalNodeChannel::new(5000000))
    );

    let proc2_to_sink = dag.connect(
        Endpoint::new(out_handle, None),
        Endpoint::new(sink_handle, None),
        Box::new(LocalNodeChannel::new(5000000))
    );

    let exec = MultiThreadedDagExecutor::new(Rc::new(dag));
    let ctx = Arc::new(MemoryExecutionContext::new());

    use std::time::Instant;
    let now = Instant::now();
    exec.start(ctx);
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}
