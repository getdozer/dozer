use serde::{Deserialize, Serialize};

use super::source::Source;
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Endpoint {
  id: String,
  name: String,
  path: String,
  enable_rest: bool,
  enable_grpc: bool,
  sql: String,
  data_maper: String,
  sources: Vec<Source>
}