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

        let mut col_idx = 0;

        let mut current_table_name = "".to_string();
        let mut fields: Vec<FieldDefinition> = vec![];
        let mut primary_index: Vec<usize> = vec![];
        for row in results {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let is_nullable: bool = row.get(2);
            let udt_name: String = row.get(3);
            let is_primary_key: bool = row.get(4);

            if current_table_name == "" {
                current_table_name = table_name.clone();
            }


            if is_primary_key {
                primary_index.push(col_idx);
            }

            if current_table_name == table_name {
                col_idx += 1;
            } else {
                schemas.push((
                    current_table_name.clone(),
                    Schema {
                        identifier: None,
                        fields: fields.clone(),
                        values: vec![],
                        primary_index,
                        secondary_indexes: vec![],
                    },
                ));
                col_idx = 0;
                primary_index = vec![];
                fields = vec![];
            }
            current_table_name = table_name;
            col_idx += 1;
            fields.push(FieldDefinition::new(
                column_name,
                convert_str_to_dozer_field_type(&udt_name),
                is_nullable,
            ));
        }
        Ok(schemas)
    }
}
