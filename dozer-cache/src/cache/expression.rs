use dozer_types::types::Field;

pub enum Expression {
    None,
    // a = 1, a containts "s", a> 4
    Simple(String, Operator, Field),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
}

pub enum Operator {
    LT,
    LTE,
    EQ,
    GT,
    GTE,
    Contains,
    MatchesAny,
    MatchesAll,
}
