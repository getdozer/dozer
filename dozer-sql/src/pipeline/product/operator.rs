use dozer_types::types::Record;
use std::fmt::{Debug, Display, Formatter, Write};

use super::JoinResult;

pub trait JoinOperator: Send + Sync {
    fn delete(&mut self, old: &Record) -> JoinResult<Record>;
    fn insert(&mut self, new: &Record) -> JoinResult<Record>;
    fn update(&mut self, old: &Record, new: &Record) -> JoinResult<Record>;
}

#[derive(Clone, Debug)]
pub enum JoinOperatorType {
    Inner {
        column_index: usize,
        interval: Duration,
    },
    Left,
    Right,
}


pub fn join_operator_factory(join_type: JoinType) -> Box<dyn JoinOperator> {
    match join_type {
        JoinType::Inner => Box::new(InnerJoinOperator::new()),
        // JoinType::Left => Box::new(LeftJoinOperator::new()),
        // JoinType::Right => Box::new(RightJoinOperator::new()),
    }
}


impl JoinOperatorType {
    pub fn execute(&self, record: &Record) -> Result<Vec<Record>, WindowError> {
        match self {
            JoinOperatorType::Inner {
                column_index,
                interval,
            } => execute_tumble_window(record, *column_index, *interval),
            WindowType::Hop {
                column_index,
                hop_size,
                interval,
            } => execute_hop_window(record, *column_index, *hop_size, *interval),
        }
    }

}