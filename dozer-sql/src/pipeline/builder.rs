use std::rc::Rc;
use std::sync::Arc;
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::comparison::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::pipeline::expression::logical::{And, Not, Or};
use crate::pipeline::expression::mathematical::{Add, Div, Mod, Mul, Sub};
use crate::pipeline::expression::operator::{Column, Expression};
use crate::pipeline::processor::selection::SelectionProcessor;
use dozer_core::dag::channel::LocalNodeChannel;
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::node::{ChannelForwarder, ExecutionContext, NextStep, Processor, Source, Sink};
use dozer_core::dag::node::NextStep::Continue;
use dozer_core::dag::dag::{Endpoint, NodeHandle, PortHandle, TestSink, TestSource};
use dozer_core::dag::executor::{MemoryExecutionContext, MultiThreadedDagExecutor};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Query, Select, SelectItem, SetExpr, Statement, UnaryOperator, Value as SqlValue};
use dozer_shared::types::{Field, Operation, OperationEvent, Record, Schema};

pub struct PipelineBuilder {
    schema: Schema
}

impl PipelineBuilder {

    pub fn new(schema: Schema) -> PipelineBuilder {
        Self {
            schema
        }
    }

    pub fn statement_to_pipeline(&self, statement: Statement) -> Result<(Dag, NodeHandle)> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    pub fn query_to_pipeline(&self, query: Query) -> Result<(Dag, NodeHandle)> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(&self, set_expr: SetExpr) -> Result<(Dag, NodeHandle)> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    fn select_to_pipeline(&self, select: Select) -> Result<(Dag, NodeHandle)> {
        // Select clause
        // let projection_processor = self.projection_to_processor(select.projection)?;

        // Where clause
        let selection_processor = self.selection_to_processor(select.selection)?;

        let mut dag = Dag::new();

        let proc_handle = dag.add_node(NodeType::Processor(selection_processor));

        Ok((dag, proc_handle))
    }

    fn selection_to_processor(&self, selection: Option<SqlExpr>) -> Result<Arc<dyn Processor>> {
        match selection {
            Some(expression) => {
                let operator = self.parse_sql_expression(expression)?;
                Ok(Arc::new(SelectionProcessor::new(0, None, None, operator)))
            }
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported WHERE clause.".to_string(),
            )),
        }
    }

    fn projection_to_processor(&self, projection: Vec<SelectItem>) -> Result<Box<dyn Processor>> {
        Err(DozerSqlError::NotImplemented(
            "Unsupported SELECT clause.".to_string(),
        ))
    }

    fn parse_sql_expression(&self, expression: SqlExpr) -> Result<Box<dyn Expression>> {
        match expression {
            SqlExpr::Identifier(ident) => {
                Ok(Box::new(Column::new(*self.schema.get_column_index(ident.value).unwrap())))
            },
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(&n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
                Ok(Box::new(s))
            },
            SqlExpr::BinaryOp { left, op, right } => {
                Ok(self.parse_sql_binary_op(*left, op, *right)?)
            },
            SqlExpr::UnaryOp { op, expr } => {
                Ok(self.parse_sql_unary_op(op, *expr)?)
            },
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(*expr)?),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Expression.".to_string(),
            )),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<Box<dyn Expression>> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(n)),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(f)),
                Err(_) => Err(DozerSqlError::NotImplemented(
                    "Value is not numeric.".to_string(),
                )),
            },
        }
    }

    fn parse_sql_unary_op(&self,
                           op: UnaryOperator,
                           expr: SqlExpr,
    ) -> Result<Box<dyn Expression>> {
        let expr_op = self.parse_sql_expression(expr)?;

            match op {
                UnaryOperator::Not => Ok(Box::new(Not::new(expr_op))),
                UnaryOperator::Plus => Err(DozerSqlError::NotImplemented(
                    "Unsupported operator PLUS.".to_string(),
                )),
                UnaryOperator::Minus => Err(DozerSqlError::NotImplemented(
                    "Unsupported operator MINUS.".to_string(),
                )),
                _ => Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported SQL unary operator {:?}", op
                ))),
            }
    }

    fn parse_sql_binary_op(&self,
        left: SqlExpr,
        op: BinaryOperator,
        right: SqlExpr,
    ) -> Result<Box<dyn Expression>> {
        let left_op = self.parse_sql_expression(left)?;
        let right_op = self.parse_sql_expression(right)?;
        match op {
            BinaryOperator::Gt => Ok(Box::new(Gt::new(left_op, right_op))),
            BinaryOperator::GtEq => Ok(Box::new(Gte::new(left_op, right_op))),
            BinaryOperator::Lt => Ok(Box::new(Lt::new(left_op, right_op))),
            BinaryOperator::LtEq => Ok(Box::new(Lte::new(left_op, right_op))),
            BinaryOperator::Eq => Ok(Box::new(Eq::new(left_op, right_op))),
            BinaryOperator::NotEq => Ok(Box::new(Ne::new(left_op, right_op))),
            BinaryOperator::Plus => Ok(Box::new(Add::new(left_op, right_op))),
            BinaryOperator::Minus => Ok(Box::new(Sub::new(left_op, right_op))),
            BinaryOperator::Multiply => Ok(Box::new(Mul::new(left_op, right_op))),
            BinaryOperator::Divide => Ok(Box::new(Div::new(left_op, right_op))),
            BinaryOperator::Modulo => Ok(Box::new(Mod::new(left_op, right_op))),
            BinaryOperator::And => Ok(Box::new(And::new(left_op, right_op))),
            BinaryOperator::Or => Ok(Box::new(Or::new(left_op, right_op))),
            BinaryOperator::BitwiseAnd => Err(DozerSqlError::NotImplemented(
                "Unsupported operator BITWISE AND.".to_string(),
            )),
            BinaryOperator::BitwiseOr => Err(DozerSqlError::NotImplemented(
                "Unsupported operator BITWISE OR.".to_string(),
            )),
            BinaryOperator::StringConcat => Err(DozerSqlError::NotImplemented(
                "Unsupported operator CONCAT.".to_string(),
            )),
            // BinaryOperator::PGRegexMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexIMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGRegexNotIMatch => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftRight => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            // BinaryOperator::PGBitwiseShiftLeft => Err(DozerSqlError::NotImplemented("Unsupported operator.".to_string())),
            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}", op
            ))),
        }
    }
}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;


pub struct SqlTestSource {
    id: i32,
    output_ports: Option<Vec<PortHandle>>
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

    fn start(&self, fw: &ChannelForwarder) -> std::result::Result<(), String>{
        for n in 0..10000000 {
            //   println!("SRC {}: Message {} received", self.id, n);
            fw.send(
                OperationEvent::new(
                    n, Operation::Insert { table_name: "test".to_string(), new: Record::new(1, vec![Field::Int(2000)]) }
                ), None
            );
        }
        fw.terminate();
        Ok(())
    }
}

pub struct SqlTestSink {
    id: i32,
    input_ports: Option<Vec<PortHandle>>
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
    let sql = "SELECT Country, COUNT(CustomerID), SUM(Spending) \
                            FROM Customers \
                            WHERE NOT (Spending >= 1000 AND Spending < 5000-Spending) \
                            GROUP BY Country \
                            HAVING COUNT(CustomerID) > 1;";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("AST: {:?}", ast);


    let statement: &Statement = &ast[0];

    let builder = PipelineBuilder::new(Schema::new(String::from("schema"), vec![String::from("Spending")], vec![Field::Int(2000)]));
    let (mut dag, proc_handle) = builder.statement_to_pipeline(statement.clone()).unwrap();

    let source = SqlTestSource::new(1,None);
    let sink = SqlTestSink::new(1, None);

    let src_handle = dag.add_node(NodeType::Source(Arc::new(source)));
    //let proc_handle = dag.add_node(NodeType::Processor(Arc::new(proc)));
    let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));


    let src_to_proc1 = dag.connect(
        Endpoint::new(src_handle, None),
        Endpoint::new(proc_handle, None),
        Box::new(LocalNodeChannel::new(5000000))
    );

    let proc1_to_sink = dag.connect(
        Endpoint::new(proc_handle, None),
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
