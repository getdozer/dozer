use crate::server::dozer_admin_grpc::{
    source_info, ConnectionInfo, HistoryType, RefreshConfig, SourceInfo,
};

use super::persistable::Persistable;
use super::pool::DbPool;
use super::schema::{self, sources};
use diesel::prelude::*;
use diesel::{insert_into, Connection, ExpressionMethods};
use schema::sources::dsl::*;
use std::error::Error;
#[derive(Queryable, PartialEq, Debug, Clone)]
#[diesel(table_name = sources)]
struct DBSource {
    id: String,
    name: String,
    dest_table_name: String,
    source_table_name: String,
    connection_id: String,
    history_type: String,
    refresh_config: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Clone)]
#[diesel(table_name = sources)]
pub struct NewSource {
    name: String,
    dest_table_name: String,
    source_table_name: String,
    connection_id: String,
    history_type: String,
    refresh_config: String,
    id: String,
}
impl TryFrom<SourceInfo> for NewSource {
    type Error = Box<dyn Error>;
    fn try_from(item: SourceInfo) -> Result<Self, Self::Error> {
        if item.connection.is_none() {
            return Err("Missing Connection props when converting from SourceInfo".to_owned())?;
        }
        let history_type_string = serde_json::to_string(&item.history_type)?;
        let refresh_config_string = serde_json::to_string(&item.refresh_config)?;
        let generated_id = uuid::Uuid::new_v4().to_string();
        let id_value = item.id.unwrap_or(generated_id);
        let connection_data = item.connection.unwrap();
        let connection_id_value = match connection_data {
            source_info::Connection::ConnectionId(connection_id_string) => {
                Some(connection_id_string)
            }
            source_info::Connection::ConnectionInfo(info) => info.id,
        };
        if connection_id_value.is_none() {
            return Err("Missing connection_id".to_owned())?;
        }
        Ok(NewSource {
            name: item.name,
            dest_table_name: item.dest_table_name,
            source_table_name: item.source_table_name,
            connection_id: connection_id_value.unwrap(),
            history_type: history_type_string,
            refresh_config: refresh_config_string,
            id: id_value,
        })
    }
}

impl TryFrom<DBSource> for SourceInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: DBSource) -> Result<Self, Self::Error> {
        let history_type_value: HistoryType = serde_json::from_str(&item.history_type)?;
        let refresh_config_value: RefreshConfig = serde_json::from_str(&item.refresh_config)?;
        let connection_value: source_info::Connection =
            source_info::Connection::ConnectionId(item.connection_id);
        Ok(SourceInfo {
            id: Some(item.id),
            name: item.name,
            dest_table_name: item.dest_table_name,
            source_table_name: item.source_table_name,
            history_type: Some(history_type_value),
            refresh_config: Some(refresh_config_value),
            connection: Some(connection_value),
        })
    }
}
impl Persistable<'_, SourceInfo> for SourceInfo {
    fn get_by_id(pool: DbPool, input_id: String) -> Result<SourceInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        let db_source: DBSource = sources
            .filter(sources::columns::id.eq(input_id))
            .first(&mut db)?;
        let source_info = SourceInfo::try_from(db_source)?;
        Ok(source_info)
    }

    fn get_multiple(pool: DbPool) -> Result<Vec<SourceInfo>, Box<dyn Error>> {
        let mut db = pool.get()?;
        let results: Vec<DBSource> = sources
            .offset(0)
            .order_by(sources::id.asc())
            .limit(100)
            .load(&mut db)?;
        let response = results
            .iter()
            .map(|result| {
                return SourceInfo::try_from(result.clone()).unwrap();
            })
            .collect();
        return Ok(response);
    }

    fn save(&mut self, pool: DbPool) -> Result<&mut SourceInfo, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn upsert(&mut self, pool: DbPool) -> Result<&mut SourceInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        if self.connection.is_none() {
            return Err("Missing connection info for Source".to_owned())?;
        }
        let connection = self.connection.as_ref().unwrap().to_owned();
        db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
            let valid_connection: Result<ConnectionInfo, Box<dyn Error>> = match connection {
                source_info::Connection::ConnectionId(connection_id_string) => {
                    // validate exists
                    let exist_connection =
                        ConnectionInfo::get_by_id(pool.to_owned(), connection_id_string)?;
                    Ok(exist_connection)
                }
                source_info::Connection::ConnectionInfo(connection_info_obj) => {
                    let mut connection_info_mutable = connection_info_obj;
                    let result = connection_info_mutable.upsert(pool.to_owned())?;
                    Ok(result.clone())
                }
            };
            let valid_connection = valid_connection?;
            self.connection = Some(source_info::Connection::ConnectionInfo(valid_connection));
            let new_source = NewSource::try_from(self.clone())?;
            insert_into(sources)
                .values(&new_source)
                .on_conflict(sources::id)
                .do_update()
                .set(&new_source)
                .execute(conn)?;
            self.id = Some(new_source.id);
            return Ok(());
        })?;
        return Ok(self);
    }
}
// impl Persistable<'_, Source> for Source {
//     fn save(&mut self, pool: DbPool) -> Result<&mut Source, Box<dyn Error>> {
//         return self.upsert(pool);
//     }

//     fn get_by_id(pool: DbPool, input_id: String) -> Result<Source, Box<dyn Error>> {
//         let mut db = pool.get()?;
//         let db_source: DBSource = sources
//             .filter(sources::columns::id.eq(input_id))
//             .first(&mut db)?;
//         let connection = models::connection::Connection::get_by_id(pool, db_source.connection_id)?;
//         let history_type_value: HistoryType = serde_json::from_str(&db_source.history_type)?;
//         let refresh_config_value: RefreshConfig = serde_json::from_str(&db_source.refresh_config)?;
//         return Ok(Source {
//             id: Some(db_source.id),
//             name: db_source.name,
//             dest_table_name: db_source.dest_table_name,
//             source_table_name: db_source.source_table_name,
//             connection,
//             history_type: history_type_value,
//             refresh_config: refresh_config_value,
//         });
//     }

//     fn get_multiple(_pool: DbPool) -> Result<Vec<Source>, Box<dyn Error>> {
//         todo!();
//     }
//     fn upsert(&mut self, pool: DbPool) -> Result<&mut Source, Box<(dyn std::error::Error + 'static)>> {
//         let history_type_value = serde_json::to_string(&self.history_type)?;
//         let refresh_config_value = serde_json::to_string(&self.refresh_config)?;
//         let db = &mut pool.get()?;
//         let generated_id = uuid::Uuid::new_v4().to_string();
//         let current_source = self.clone();
//         let valid_id = current_source.id.unwrap_or(generated_id);
//         let mut cloned_connection = self.connection.clone();
//         db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
//             cloned_connection.upsert(pool)?;
//             let valid_connection_id = cloned_connection.id;
//             let new_source = NewSource {
//                 name: current_source.name.clone(),
//                 dest_table_name: current_source.dest_table_name.clone(),
//                 source_table_name: current_source.source_table_name.clone(),
//                 connection_id: valid_connection_id.unwrap(),
//                 history_type: history_type_value,
//                 refresh_config: refresh_config_value,
//                 id: valid_id.clone(),
//             };
//             insert_into(sources)
//                 .values(&new_source)
//                 .on_conflict(sources::id)
//                 .do_update()
//                 .set(&new_source)
//                 .execute(conn)?;
//             return Ok(());
//         })?;
//         self.id = Some(valid_id);
//         return Ok(self);
//     }
// }
