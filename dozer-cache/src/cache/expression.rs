use dozer_types::types::Field;
pub struct QueryExpression {
    pub filter: FilterExpression,
    pub order_by: Vec<SortOptions>,
    pub limit: usize,
    pub skip: usize,
}

impl QueryExpression {
    pub fn new(
        filter: FilterExpression,
        order_by: Vec<SortOptions>,
        limit: usize,
        skip: usize,
    ) -> Self {
        Self {
            filter,
            order_by,
            limit,
            skip,
        }
    }
}

pub enum FilterExpression {
    None,
    // a = 1, a containts "s", a> 4
    Simple(String, Operator, Field),
    And(Box<FilterExpression>, Box<FilterExpression>),
    Or(Box<FilterExpression>, Box<FilterExpression>),
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

pub enum SortDirection {
    Ascending,
    Descending,
}
pub struct SortOptions {
    pub field_name: String,
    pub direction: SortDirection,
}
