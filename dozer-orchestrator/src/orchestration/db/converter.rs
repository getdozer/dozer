use super::models::connection::Connection as DbConnection;
use crate::orchestration::models::connection::{Authentication, Connection, DBType};
use serde_json::Value;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use uuid::Uuid;

impl Display for DBType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for DBType {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<DBType, Self::Err> {
        match s {
            "Postgres" | "postgres" => Ok(DBType::Postgres),
            "Databricks" | "databricks" => Ok(DBType::Databricks),
            "Snowflake" | "snowflake" => Ok(DBType::Snowflake),
            _ => Err("Not match any value in Enum"),
        }
    }
}

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

impl TryFrom<Connection> for DbConnection {
    type Error = Box<dyn Error>;
    fn try_from(item: Connection) -> Result<Self, Self::Error> {
        let authentication = item.authentication;
        let mut json: Value = serde_json::to_value(&authentication)
            .map_err(|err| string_to_static_str(err.to_string()))?;
        json["name"] = serde_json::Value::String(item.name);
        let db_type = item.db_type.to_string();
        let id = item.id.unwrap_or(Uuid::new_v4().to_string());
        return Ok(DbConnection {
            id: id,
            auth: json.to_string(),
            db_type: db_type,
        });
    }
}

impl TryFrom<DbConnection> for Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let authentication_str = item.auth;
        let json: Value = serde_json::from_str(&authentication_str)
            .map_err(|err| string_to_static_str(err.to_string()))?;
        let name = json["name"].as_str().unwrap();
        let db_type = DBType::from_str(json["db_type"].as_str().unwrap())
            .map_err(|err| string_to_static_str(err.to_string()))?;
        let user = json["user"].as_str().unwrap().to_string();
        let password = json["password"].as_str().unwrap().to_string();
        let host = json["host"].as_str().unwrap().to_string();
        let database = json["host"].as_str().unwrap().to_string();
        let port = json["port"]
            .as_str()
            .unwrap()
            .parse::<u32>()
            .map_err(|err| string_to_static_str(err.to_string()))?;

        match db_type {
            DBType::Postgres => {
                let authentication = Authentication::PostgresAuthentication {
                    user,
                    password,
                    host,
                    port,
                    database,
                };
                return Ok(Connection {
                    db_type: db_type,
                    authentication: authentication,
                    name: name.to_owned(),
                    id: Some(item.id),
                });
            }
            DBType::Databricks => todo!(),
            DBType::Snowflake => todo!(),
        }
    }
}
