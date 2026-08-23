#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
}

impl Expression {
    pub fn return_type(&self) -> DataType {
        match self {
            Expression::InList { .. } => DataType::Boolean,
            _ => DataType::Boolean,
        }
    }
