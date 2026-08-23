#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_list_eval() {
        let record = Record::new(vec![Field::Int(10)]);
        let schema = Schema::default();
        let expr = Expression::InList {
            expr: Box::new(Expression::Column(0)),
            list: vec![Expression::Literal(Field::Int(5)), Expression::Literal(Field::Int(10))],
            negated: false,
        };
        let res = expr.evaluate(&record, &schema).unwrap();
        assert_eq!(res, Field::Boolean(true));
    }

    #[test]
    fn test_not_in_list_eval() {
        let record = Record::new(vec![Field::Int(20)]);
        let schema = Schema::default();
        let expr = Expression::InList {
            expr: Box::new(Expression::Column(0)),
            list: vec![Expression::Literal(Field::Int(5)), Expression::Literal(Field::Int(10))],
            negated: true,
        };
        let res = expr.evaluate(&record, &schema).unwrap();
        assert_eq!(res, Field::Boolean(true));
    }
