use std::collections::HashMap;

use anyhow::Context;
use dozer_types::types::{FieldDefinition, Schema, SchemaIdentifier};

use crate::connectors::connector::TableInfo;

use super::helper::{self, convert_str_to_dozer_field_type};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct SchemaHelper {
    pub conn_str: String,
}
impl SchemaHelper {
    pub fn get_tables(&mut self) -> anyhow::Result<Vec<TableInfo>> {
        let result_vec = self.get_schema()?;

        let mut arr = vec![];
        for (name, schema) in result_vec.iter() {
            let columns: Vec<String> = schema.fields.iter().map(|f| f.name.clone()).collect();
            arr.push(TableInfo {
                name: name.clone(),
                id: schema
                    .identifier
                    .clone()
                    .context("schema.id is expected in schema helper")?
                    .id,
                columns: Some(columns),
            });
        }

        Ok(arr)
    }

    pub fn get_schema(&mut self) -> anyhow::Result<Vec<(String, Schema)>> {
        let mut client = helper::connect(self.conn_str.clone())?;

        let mut schemas: Vec<(String, Schema)> = Vec::new();

        let results = client.query(SQL, &[])?;

        let mut map: HashMap<String, (Vec<FieldDefinition>, Vec<bool>, u32)> = HashMap::new();
        results
            .iter()
            .map(|row| {
                let table_name: String = row.get(0);
                let column_name: String = row.get(1);
                let is_nullable: bool = row.get(2);
                let udt_name: String = row.get(3);
                let is_primary_key: bool = row.get(4);
                let table_id: u32 = if let Some(rel_id) = row.get(5) {
                    rel_id
                } else {
                    let mut s = DefaultHasher::new();
                    table_name.hash(&mut s);
                    s.finish() as u32
                };
                (
                    table_name,
                    FieldDefinition::new(
                        column_name,
                        convert_str_to_dozer_field_type(&udt_name),
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

            // TODO:: Sometimes there is no primary index ?
            // let primary_index =   if primary_index.
            let primary_index = if primary_index.len() < 1 {
                vec![0]
            } else {
                primary_index
            };

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

const SQL: &str= "SELECT 
  table_info.table_name, 
  table_info.column_name, 
  case when table_info.is_nullable = 'NO' then false else true end as is_nullable, 
  table_info.udt_name, 
  constraint_info.constraint_type is not null as is_primary_key,
  st_user_table.relid
FROM 
  (
    SELECT 
      table_schema, 
      table_catalog, 
      table_name, 
      column_name, 
      is_nullable, 
      data_type, 
      numeric_precision, 
      udt_name, 
      character_maximum_length 
    from 
      information_schema.columns 
    where 
      table_name in (
        select 
          table_name 
        from 
          information_schema.tables 
        where 
          table_schema = 'public' 
        ORDER BY 
          table_name
      ) 
    order by 
      table_name
  ) table_info 
  LEFT JOIN pg_catalog.pg_statio_user_tables st_user_table ON st_user_table.relname = table_info.table_name
  left join (
    select 
      constraintUsage.table_name, 
      constraintUsage.column_name, 
      table_constraints.constraint_name, 
      table_constraints.constraint_type 
    from 
      information_schema.constraint_column_usage constraintUsage 
      join information_schema.table_constraints table_constraints on constraintUsage.table_name = table_constraints.table_name 
      and constraintUsage.constraint_name = table_constraints.constraint_name 
      and table_constraints.constraint_type = 'PRIMARY KEY'
  ) constraint_info on table_info.table_name = constraint_info.table_name 

  and table_info.column_name = constraint_info.column_name 
order by 
  table_info.table_schema, 
  table_info.table_catalog, 
  table_info.table_name;";
