
use std::convert::From;
use crate::models::Connection as ConnectionDTO;
#[derive(Queryable, PartialEq, Clone, Debug)]
pub struct Connection { 
  pub id: i32,
  pub auth: String,
  pub db_type: String,
}

impl From<ConnectionDTO> for Number {
  fn from(item: ConnectionDTO) -> Self {
    Connection {}
  }
} 