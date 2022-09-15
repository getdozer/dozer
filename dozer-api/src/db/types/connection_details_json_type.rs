use diesel::backend::Backend;
use diesel::deserialize;
use diesel::serialize;
use diesel::types::{FromSql, ToSql};

use diesel::sqlite::Sqlite;
use diesel::{serialize::Output, sql_types::Text};
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(AsExpression, Debug, Deserialize, Serialize, FromSqlRow, PartialEq, Clone)]
#[sql_type = "Text"]
pub struct ConnectionDetailsJsonType(serde_json::Value);

impl FromSql<Text, Sqlite> for ConnectionDetailsJsonType {
    fn from_sql(
        bytes: Option<&<diesel::sqlite::Sqlite as Backend>::RawValue>,
    ) -> deserialize::Result<Self> {
        let t = <String as FromSql<Text, Sqlite>>::from_sql(bytes)?;
        Ok(Self(serde_json::from_str(&t)?))
    }
}

impl ToSql<Text, Sqlite> for ConnectionDetailsJsonType {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Sqlite>) -> serialize::Result {
        let s = serde_json::to_string(&self.0)?;
        <String as ToSql<Text, Sqlite>>::to_sql(&s, out)
    }
}

pub mod exports {
    pub use super::ConnectionDetailsJsonType;
}
