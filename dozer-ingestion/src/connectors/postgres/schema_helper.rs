use dozer_shared::types::{ColumnInfo, TableInfo};
use tokio_postgres::{ NoTls};

pub struct SchemaHelper {
    pub conn_str: String,
}
impl SchemaHelper {
    async fn _connect(&mut self) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
                panic!("Connection failed!");
            }
        });
        client
    }

    pub async fn get_schema(&mut self) -> Vec<TableInfo> {
        let client = self._connect().await;
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
        let mut table_schema: Vec<TableInfo> = vec![];

        let results = client.query(query, &[]).await.unwrap();

        let mut current_table: String = String::from("");

        for row in results {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let is_nullable: bool = row.get(2);
            let udt_name = row.get(3);
            let is_primary_key: bool = row.get(4);
            
            let column_info = ColumnInfo {
                column_name,
                is_nullable,
                udt_name,
                is_primary_key,
            };
            if current_table != table_name {
                current_table = table_name.clone();
                let mut column_vec: Vec<ColumnInfo> = Vec::new();
                column_vec.push(column_info);
                table_schema.push(TableInfo {
                    table_name: table_name.clone(),
                    columns: column_vec,
                })
            } else {
                let mut current_table_info = table_schema.pop().unwrap();
                current_table_info.columns.push(column_info);
                table_schema.push(current_table_info);
            }
        }
        table_schema
    }
}
