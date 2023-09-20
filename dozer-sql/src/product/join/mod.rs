use crate::errors::JoinError;

pub mod factory;

pub(crate) mod operator;
mod processor;

type JoinResult<T> = Result<T, JoinError>;
