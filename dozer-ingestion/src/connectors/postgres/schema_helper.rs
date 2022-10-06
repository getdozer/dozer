use std::collections::HashMap;

use dozer_types::types::{FieldDefinition, Schema};

use super::helper::{self, convert_str_to_dozer_field_type};

pub struct SchemaHelper {
    pub conn_str: String,
}

impl SchemaHelper {
    pub fn get_schema(&mut self) -> anyhow::Result<Vec<(String, Schema)>> {
        let mut client = helper::connect(self.conn_str.clone())?;
        let query = "select genericInfo.table_name, genericInfo.column_name, case when genericInfo.is_nullable = 'NO' then false else true end as is_nullable , genericInfo.udt_name, keyInfo.constraint_type is not null as is_primary_key
        FROM
        (SELECT table_schema, table_catalog, table_name, column_name, is_nullable , data_type , numeric_precision , udt_name, character_maximum_length from  information_schema.columns 
         where table_name  in ( select  table_name from information_schema.tables where table_schema = 'public' ORDER BY table_name)
         order by table_name) genericInfo
         
         left join  
         
         (select constraintUsage.table_name , constraintUsage.column_name , tableconstraints.constraint_name, tableConstraints.constraint_type from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE constraintUsage join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tableConstraints on constraintUsage.table_name = tableConstraints.table_name  and  constraintUsage.constraint_name
        =    tableConstraints.constraint_name and tableConstraints.constraint_type = 'PRIMARY KEY') keyInfo
        
       on genericInfo.table_name = keyInfo.table_name and genericInfo.column_name = keyInfo.column_name
       order by genericInfo.table_schema, genericInfo.table_catalog, genericInfo.table_name, genericInfo.column_name";
        let mut schemas: Vec<(String, Schema)> = Vec::new();

        let results = client.query(query, &[])?;

        let mut map: HashMap<String, (Vec<FieldDefinition>, Vec<bool>)> = HashMap::new();
        results
            .iter()
            .map(|row| {
                let table_name: String = row.get(0);
                let column_name: String = row.get(1);
                let is_nullable: bool = row.get(2);
                let udt_name: String = row.get(3);
                let is_primary_key: bool = row.get(4);
                (
                    table_name,
                    FieldDefinition::new(
                        column_name,
                        convert_str_to_dozer_field_type(&udt_name),
                        is_nullable,
                    ),
                    is_primary_key,
                )
            })
            .for_each(|row| {
                let (table_name, field_def, is_primary_key) = row;

                let vals = map.get(&table_name);
                let (mut fields, mut primary_keys) = match vals {
                    Some((fields, primary_keys)) => (fields.clone(), primary_keys.clone()),
                    None => (vec![], vec![]),
                };

                fields.push(field_def);
                primary_keys.push(is_primary_key);
                map.insert(table_name, (fields, primary_keys));
            });

        for (table_name, (fields, primary_keys)) in map.into_iter() {
            let primary_index = primary_keys
                .iter()
                .enumerate()
                .filter(|(_, b)| **b)
                .map(|(idx, _)| idx)
                .collect();

            let schema = Schema {
                identifier: None,
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
