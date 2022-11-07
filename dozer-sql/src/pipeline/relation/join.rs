#![allow(dead_code)]

use crate::pipeline::expression::execution::Expression;
use dozer_core::dag::node::PortHandle;

pub enum Relation {
    Simple(SimpleRelationRules),
    Join(JoinRelationRules),
}

pub enum JoinType {
    Left,
    Right,
    Inner,
    Full,
}

pub struct JoinRelationRules {
    alias: Option<String>,
    left: Box<Relation>,
    left_key: Expression,
    right: Box<Relation>,
    right_key: Expression,
    typ: JoinType,
}

pub struct SimpleRelationRules {
    name: String,
    handle: PortHandle,
}
