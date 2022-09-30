use dozer_types::types::Field;

pub enum Expression {
    // a = 1
    Simple(String, Comparator, Field),
    // OR, exp1, exp2
    Combination(Operator, Box<Expression>, Box<Expression>),
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
