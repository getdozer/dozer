use super::schema::apps;
use diesel::{AsChangeset, Identifiable, Insertable, Queryable};
use dozer_types::chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = apps)]
pub struct Application {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) config: String,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = apps)]
pub struct NewApplication {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) config: String,
}
