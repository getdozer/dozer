use super::schema::apps;
use diesel::{AsChangeset, Identifiable, Insertable, Queryable};
use serde::{Deserialize, Serialize};
#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = apps)]
pub struct Application {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) config: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = apps)]
pub struct NewApplication {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) config: String,
}
