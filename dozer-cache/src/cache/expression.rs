use dozer_types::types::Field;

pub enum Expression {
    None,
    // a = 1
    Simple(String, Comparator, Field),
    // OR, exp1, exp2
    Composite(Operator, Box<Expression>, Box<Expression>),
}

pub enum Operator {
    AND,
    OR,
}

pub enum Comparator {
    LT,
    LTE,
    EQ,
    GT,
    GTE,
}
