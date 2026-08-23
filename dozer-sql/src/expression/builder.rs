impl ExpressionBuilder {
    pub fn parse_expr(&self, expr: &Expr, schema: &Schema) -> Result<Expression, Error> {
        match expr {
            Expr::InList { expr, list, negated } => {
                let parsed_expr = self.parse_expr(expr, schema)?;
                let parsed_list = list
                    .iter()
                    .map(|item| self.parse_expr(item, schema))
                    .collect::<Result<Vec<_>, Error>>()?;
                Ok(Expression::InList {
                    expr: Box::new(parsed_expr),
                    list: parsed_list,
                    negated: *negated,
                })
            }
            Expr::InSubquery { expr, subquery, negated } => {
                Err(Error::SubqueryInWhereRequiresJoinRewrite)
            }
            _ => Err(Error::UnsupportedExpression(expr.to_string())),
        }
    }
