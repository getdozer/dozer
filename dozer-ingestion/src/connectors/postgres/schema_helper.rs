use std::collections::HashMap;

use dozer_types::errors::connector::ConnectorError;
use dozer_types::types::{FieldDefinition, Schema, SchemaIdentifier};

use crate::connectors::connector::TableInfo;

use super::helper;
use crate::connectors::postgres::helper::postgres_type_to_dozer_type;
use postgres_types::Type;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct SchemaHelper {
    pub conn_str: String,
}
impl SchemaHelper {
    pub fn get_tables(&mut self) -> Result<Vec<TableInfo>, ConnectorError> {
        let result_vec = self.get_schema()?;

        let mut arr = vec![];
        for (name, schema) in result_vec.iter() {
            let columns: Vec<String> = schema.fields.iter().map(|f| f.name.clone()).collect();
            arr.push(TableInfo {
                name: name.clone(),
                id: schema.identifier.clone().unwrap().id,
                columns: Some(columns),
            });
        }

        Ok(arr)
    }

    pub fn get_schema(&mut self) -> Result<Vec<(String, Schema)>, ConnectorError> {
        let mut client = helper::connect(self.conn_str.clone())?;

        let mut schemas: Vec<(String, Schema)> = Vec::new();

        let results = client
            .query(SQL, &[])
            .map_err(|_| ConnectorError::InvalidQueryError)?;

        let mut map: HashMap<String, (Vec<FieldDefinition>, Vec<bool>, u32)> = HashMap::new();
        results
            .iter()
            .map(|row| {
                let table_name: String = row.get(0);
                let column_name: String = row.get(1);
                let is_nullable: bool = row.get(2);
                let is_primary_key: bool = row.get(3);
                let table_id: u32 = if let Some(rel_id) = row.get(4) {
                    rel_id
                } else {
                    let mut s = DefaultHasher::new();
                    table_name.hash(&mut s);
                    s.finish() as u32
                };
                let type_oid: u32 = row.get(6);
                (
                    table_name,
                    FieldDefinition::new(
                        column_name,
                        postgres_type_to_dozer_type(Type::from_oid(type_oid)),
                        is_nullable,
                    ),
                    is_primary_key,
                    table_id,
                )
            })
            .for_each(|row| {
                let (table_name, field_def, is_primary_key, table_id) = row;

                let vals = map.get(&table_name);
                let (mut fields, mut primary_keys, table_id) = match vals {
                    Some((fields, primary_keys, table_id)) => {
                        (fields.clone(), primary_keys.clone(), *table_id)
                    }
                    None => (vec![], vec![], table_id),
                };

                fields.push(field_def);
                primary_keys.push(is_primary_key);
                map.insert(table_name, (fields, primary_keys, table_id));
            });

        for (table_name, (fields, primary_keys, table_id)) in map.into_iter() {
            let primary_index: Vec<usize> = primary_keys
                .iter()
                .enumerate()
                .filter(|(_, b)| **b)
                .map(|(idx, _)| idx)
                .collect();

            let schema = Schema {
                identifier: Some(SchemaIdentifier {
                    id: table_id,
                    version: 1,
                }),
                fields: fields.clone(),
                values: vec![],
                primary_index,
                secondary_indexes: vec![],
            };
            schemas.push((table_name, schema));
        }

        Ok(schemas)
    }
}

const SQL: &str= "
SELECT table_info.table_name,
       table_info.column_name,
       CASE WHEN table_info.is_nullable = 'NO' THEN false ELSE true END AS is_nullable,
       constraint_info.constraint_type IS NOT NULL                      AS is_primary_key,
       st_user_table.relid,
       pc.relreplident,
       pt.oid AS type_oid
FROM (SELECT table_schema,
             table_catalog,
             table_name,
             column_name,
             is_nullable,
             data_type,
             numeric_precision,
             udt_name,
             character_maximum_length
      FROM information_schema.columns
      WHERE table_name IN (SELECT table_name
                           FROM information_schema.tables
                           WHERE table_schema = 'public'
                           ORDER BY table_name)
      ORDER BY table_name) table_info
         LEFT JOIN pg_catalog.pg_statio_user_tables st_user_table ON st_user_table.relname = table_info.table_name
         LEFT JOIN (SELECT constraintUsage.table_name,
                           constraintUsage.column_name,
                           table_constraints.constraint_name,
                           table_constraints.constraint_type
                    FROM information_schema.constraint_column_usage constraintUsage
                             JOIN information_schema.table_constraints table_constraints
                                  ON constraintUsage.table_name = table_constraints.table_name
                                      AND constraintUsage.constraint_name = table_constraints.constraint_name
                                      AND table_constraints.constraint_type = 'PRIMARY KEY') constraint_info
                   ON table_info.table_name = constraint_info.table_name
                       AND table_info.column_name = constraint_info.column_name
--          LEFT JOIN pg_
         LEFT JOIN pg_class pc ON st_user_table.relid = pc.oid
         LEFT JOIN pg_type pt ON table_info.udt_name = pt.typname
-- LEFT JOIN pg_att
ORDER BY table_info.table_schema,
         table_info.table_catalog,
         table_info.table_name;";
