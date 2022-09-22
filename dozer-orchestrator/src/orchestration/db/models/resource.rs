use serde::{Deserialize, Serialize};
#[derive(Queryable, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Resource {
    id: String,
    name: String,
    dest_table_name: String,
    connection_id: String,
    history_type: String,
    refresh_config: String
}
