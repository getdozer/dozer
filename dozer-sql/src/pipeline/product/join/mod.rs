use crate::pipeline::errors::JoinError;

pub mod factory;

mod operator;
mod processor;

type JoinResult<T> = Result<T, JoinError>;
