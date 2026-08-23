impl Expression {
    pub fn evaluate(&self, record: &Record, schema: &Schema) -> Result<Field, Error> {
        match self {
            Expression::InList { expr, list, negated } => {
                let target = expr.evaluate(record, schema)?;
                if target.is_null() {
                    return Ok(Field::Null);
                }
                let mut matched = false;
                for item in list {
                    let item_val = item.evaluate(record, schema)?;
                    if target == item_val {
                        matched = true;
                        break;
                    }
                }
                Ok(Field::Boolean(if *negated { !matched } else { matched }))
            }
            _ => Err(Error::UnsupportedEvaluation),
        }
