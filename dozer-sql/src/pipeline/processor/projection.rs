use std::collections::HashMap;
use std::sync::Arc;
use dozer_core::dag::node::NextStep;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::node::{Processor, ExecutionContext, ChannelForwarder};
use dozer_types::types::{Field, OperationEvent, Schema};
use sqlparser::ast::{BinaryOperator, SelectItem, Expr as SqlExpr, Value as SqlValue, FunctionArg, FunctionArgExpr};
use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};
use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::expression::Expression::{AggregateFunction, ScalarFunction};
use crate::pipeline::expression::operator::OperatorType;
use crate::pipeline::expression::scalar::ScalarFunctionType;

pub struct ProjectionProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
    expressions: Vec<Box<Expression>>,
}

impl ProjectionProcessor {
    pub fn new(id: i32, input_ports: Option<Vec<PortHandle>>, output_ports: Option<Vec<PortHandle>>, expressions: Vec<Box<Expression>>) -> Self {
        Self { id, input_ports, output_ports, expressions }
    }
}

impl Processor for ProjectionProcessor {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> core::result::Result<(), String> {
        println!("PROC {}: Initialising SelectionProcessor", self.id);
        Ok(())
    }

    fn process(&self, from_port: Option<PortHandle>, op: OperationEvent, ctx: & dyn ExecutionContext, fw: &ChannelForwarder) -> core::result::Result<NextStep, String> {

        fw.send(op, None);
        Ok(NextStep::Continue)
    }
}

pub struct ProjectionBuilder {
    schema_idx: HashMap<String, usize>,
    expression_builder: ExpressionBuilder,
}

impl ProjectionBuilder {

    pub fn new(schema: &Schema) -> ProjectionBuilder {
        Self {
            schema_idx: schema.fields.iter().enumerate().map(|e| (e.1.name.clone(), e.0)).collect(),
            expression_builder: ExpressionBuilder::new(schema.clone())
        }
    }

    pub fn get_processor(&self, projection: Vec<SelectItem>) -> Result<Arc<dyn Processor>> {
        let expressions = projection.into_iter()
            .map(|expr| {
                self.parse_sql_select_item(&expr)
            })
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Box<Expression>>>>()?;
        Ok(Arc::new(ProjectionProcessor::new(0, None, None, expressions)))
    }

    fn parse_sql_select_item(
        &self,
        sql: &SelectItem,
    ) -> Result<Vec<Box<Expression>>> {

        match sql {
            SelectItem::UnnamedExpr(sql_expr) => {
                let expr = self.parse_sql_expression(sql_expr);
                Ok(vec![expr.unwrap()])
            },
            SelectItem::ExprWithAlias { expr, alias } => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Expression {}", expr)
            )),
            SelectItem::Wildcard => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Wildcard")
            )),
            SelectItem::QualifiedWildcard(ref object_name) => Err(DozerSqlError::NotImplemented(
                format!("Unsupported Qualified Wildcard {}", object_name)
            ))
        }
    }

    fn parse_sql_expression(&self, expression: &SqlExpr) -> Result<Box<Expression>> {

        match expression {
            SqlExpr::Identifier(ident) => {
               Ok(Box::new(Expression::Column{ index: *self.schema_idx.get(&ident.value).unwrap()}))
            },
            SqlExpr::Value(SqlValue::Number(n, _)) => Ok(self.parse_sql_number(&n)?),
            SqlExpr::Value(SqlValue::SingleQuotedString(s) |
                           SqlValue::DoubleQuotedString(s)) => Ok(Box::new(Expression::Literal(Field::String(s.clone())))),
            SqlExpr::BinaryOp { left, op, right } => Ok(self.parse_sql_binary_op(left, op, right)?),
            SqlExpr::Nested(expr) => Ok(self.parse_sql_expression(expr)?),
            SqlExpr::Function(sql_function) => {
                let name = sql_function.name.to_string().to_lowercase();

                if let Ok(function) = ScalarFunctionType::new(&name) {
                    let args = sql_function.clone().args.into_iter()
                        .map(|a| self.parse_sql_function_arg(&a))
                        .collect::<Result<Vec<Box<Expression>>>>()?;
                    return Ok(Box::new(ScalarFunction { fun: function, args }));
                };

                if let Ok(function) = AggregateFunctionType::new(&name) {
                    let args = sql_function.clone().args.into_iter()
                        .map(|a| self.parse_sql_function_arg(&a))
                        .collect::<Result<Vec<Box<Expression>>>>()?;
                    return Ok(Box::new(AggregateFunction { fun: function, args }));
                };
                Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Expression: {:?}", expression,
                )))
            }

            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported Expression: {:?}", expression,
            ))),
        }
    }

    fn parse_sql_function_arg(&self, argument: &FunctionArg) -> Result<Box<Expression>> {
            match argument {
                FunctionArg::Named {
                    name: _,
                    arg: FunctionArgExpr::Expr(arg),
                } => self.parse_sql_expression(arg),
                FunctionArg::Named {
                    name: _,
                    arg: FunctionArgExpr::Wildcard,
                } => Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported qualified wildcard argument: {:?}",
                    argument
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                    self.parse_sql_expression(arg)
                }
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported qualified wildcard argument: {:?}",
                    argument
                ))),
                _ => Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported qualified wildcard argument: {:?}",
                    argument
                )))
            }
        }


        fn parse_sql_binary_op(&self,
                           left: &SqlExpr,
                           op: &BinaryOperator,
                           right: &SqlExpr,
    ) -> Result<Box<Expression>> {
        let left_op = self.parse_sql_expression(left)?;
        let right_op = self.parse_sql_expression(right)?;
        match op {

            BinaryOperator::Plus => Ok(Box::new(Expression::Binary {left:left_op, operator: OperatorType::Sum, right:right_op})),

            _ => Err(DozerSqlError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}", op
            ))),
        }
    }

    fn parse_sql_number(&self, n: &str) -> Result<Box<Expression>> {
        match n.parse::<i64>() {
            Ok(n) => Ok(Box::new(Expression::Literal(Field::Int(n)))),
            Err(_) => match n.parse::<f64>() {
                Ok(f) => Ok(Box::new(Expression::Literal(Field::Float(f)))),
                Err(_) => Err(DozerSqlError::NotImplemented(format!(
                    "Value is not Numeric.",
                ))),
            },
        }
    }

}