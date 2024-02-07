use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use dozer_core::{
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    chrono::{self, DateTime, NaiveDate, Utc},
    errors::internal::BoxedError,
    log::info,
    models::ingestion_types::OracleConfig,
    thiserror::Error,
    tonic::async_trait,
    types::{Field, FieldType, Record, Schema},
};
use oracle::{
    sql_type::{OracleType, ToSql},
    Connection,
};

#[derive(Error, Debug)]
enum Error {
    #[error("Updating a primary key is not supported. Old: {:?}, new: {:?}", .old, .new)]
    UpdatedPrimaryKey { old: Vec<Field>, new: Vec<Field> },
}

#[derive(Debug)]
struct BatchedOperation {
    op_kind: OpKind,
    params: Record,
}

#[derive(Debug)]
struct OracleSink {
    conn: Connection,
    insert_append: String,
    pk: Vec<usize>,
    field_types: Vec<FieldType>,
    merge_statement: String,
    batch_params: Vec<BatchedOperation>,
    batch_size: usize,
    last_commit: Instant,
}

#[derive(Debug)]
pub struct OracleSinkFactory {
    pub config: OracleConfig,
    pub table: String,
}

fn generate_merge_statement(table_name: &str, schema: &Schema) -> String {
    let field_names = schema.fields.iter().map(|field| &field.name);
    let mut parameter_index = 1usize..;
    let input_fields = field_names
        .clone()
        .zip(&mut parameter_index)
        .map(|(name, i)| format!(":{i} \"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let destination_columns = field_names
        .clone()
        .map(|name| format!("D.\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let source_values = field_names
        .clone()
        .map(|name| format!("S.\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let destination_assign = field_names
        .clone()
        .map(|name| format!("D.\"{name}\" = S.\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let pk_select = schema
        .primary_index
        .iter()
        .map(|ix| &schema.fields[*ix].name)
        .map(|name| format!("D.\"{name}\" = S.\"{name}\""))
        .collect::<Vec<_>>()
        .join(" AND ");
    let opkind_idx = parameter_index.next().unwrap();
    format!(
        r#"MERGE INTO "{table_name}" D
        USING (SELECT {input_fields}, :{opkind_idx} DOZER_OPKIND FROM DUAL) S
        ON (S.DOZER_OPKIND > 0)
        WHEN NOT MATCHED THEN INSERT ({destination_columns}) VALUES ({source_values})
        WHEN MATCHED THEN UPDATE SET {destination_assign} WHERE {pk_select}
        DELETE WHERE S.DOZER_OPKIND = 2
        "#
    )
}

#[async_trait]
impl SinkFactory for OracleSinkFactory {
    fn type_name(&self) -> String {
        "oracle".to_string()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn build(
        &self,
        mut input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn dozer_core::node::Sink>, BoxedError> {
        let config = &self.config;
        let root_connect_string = format!("{}:{}/{}", config.host, config.port, config.sid);
        let connection = Connection::connect(&config.user, &config.password, root_connect_string)?;

        let schema = input_schemas.remove(&DEFAULT_PORT_HANDLE).unwrap();

        let table_name = &self.table;
        let mut column_defs = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let name = &field.name;
            let col_type = match field.typ {
                dozer_types::types::FieldType::UInt => "NUMBER",
                dozer_types::types::FieldType::U128 => unimplemented!(),
                dozer_types::types::FieldType::Int => "NUMBER",
                dozer_types::types::FieldType::I128 => unimplemented!(),
                // Should this be BINARY_DOUBLE?
                dozer_types::types::FieldType::Float => "NUMBER",
                dozer_types::types::FieldType::Boolean => "NUMBER",
                dozer_types::types::FieldType::String => "VARCHAR2(2000)",
                dozer_types::types::FieldType::Text => "VARCHAR2(2000)",
                dozer_types::types::FieldType::Binary => "RAW(1000)",
                dozer_types::types::FieldType::Decimal => unimplemented!(),
                dozer_types::types::FieldType::Timestamp => "TIMESTAMP(9) WITH TIME ZONE",
                dozer_types::types::FieldType::Date => "TIMESTAMP(0)",
                dozer_types::types::FieldType::Json => unimplemented!(),
                dozer_types::types::FieldType::Point => unimplemented!("Oracle Point"),
                dozer_types::types::FieldType::Duration => unimplemented!(),
            };
            column_defs.push(format!(
                "\"{name}\" {col_type}{}",
                if field.nullable { "" } else { " NOT NULL" }
            ));
        }
        let table = format!(
            "CREATE TABLE \"{table_name}\" ({})",
            column_defs.join(",\n")
        );
        info!("### CREATE TABLE #### \n: {:?}", table);
        connection.execute(&table, &[])?;

        let insert_append = format!(
            "INSERT /*+ APPEND */ INTO \"{table_name}\" VALUES ({})",
            (1..=schema.fields.len())
                .map(|i| format!(":{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let field_types = schema.fields.iter().map(|field| field.typ).collect();
        Ok(Box::new(OracleSink {
            conn: connection,
            insert_append,
            merge_statement: generate_merge_statement(table_name, &schema),
            field_types,
            pk: schema.primary_index,
            batch_params: Vec::new(),
            //TODO: make this configurable
            batch_size: 1000,
            last_commit: Instant::now(),
        }))
    }
}

#[derive(Debug)]
struct OraField(Field, FieldType);

impl ToSql for OraField {
    fn oratype(&self, conn: &Connection) -> oracle::Result<oracle::sql_type::OracleType> {
        match &self.0 {
            Field::UInt(v) => v.oratype(conn),
            Field::Int(v) => v.oratype(conn),
            Field::Float(v) => v.oratype(conn),
            Field::Boolean(_) => Ok(OracleType::Number(1, 0)),
            Field::String(v) | Field::Text(v) => v.oratype(conn),
            Field::Binary(v) => v.oratype(conn),
            Field::Timestamp(v) => v.oratype(conn),
            Field::Date(v) => v.oratype(conn),
            Field::Duration(_) => Ok(OracleType::IntervalDS(9, 9)),
            Field::Null => match self.1 {
                FieldType::UInt => 0u64.oratype(conn),
                FieldType::Int => 0i64.oratype(conn),
                FieldType::Float => 0f64.oratype(conn),
                FieldType::Boolean => Ok(OracleType::Number(1, 0)),
                FieldType::String | FieldType::Text => "".oratype(conn),
                FieldType::Binary => Vec::<u8>::new().oratype(conn),
                FieldType::Timestamp => DateTime::<Utc>::MAX_UTC.oratype(conn),
                FieldType::Date => NaiveDate::MAX.oratype(conn),
                FieldType::Duration => Ok(OracleType::IntervalDS(9, 9)),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    fn to_sql(&self, val: &mut oracle::SqlValue) -> oracle::Result<()> {
        match &self.0 {
            Field::UInt(v) => v.to_sql(val),
            Field::Int(v) => v.to_sql(val),
            Field::Float(v) => v.to_sql(val),
            Field::Boolean(_) => 1.to_sql(val),
            Field::String(v) | Field::Text(v) => v.to_sql(val),
            Field::Binary(v) => v.to_sql(val),
            Field::Timestamp(v) => v.to_sql(val),
            Field::Date(v) => v.to_sql(val),
            Field::Duration(d) => chrono::Duration::from_std(d.0)
                .map_err(|e| oracle::Error::OutOfRange(e.to_string()))
                .and_then(|v| v.to_sql(val)),
            Field::Null => val.set_null(),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
enum OpKind {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

impl OracleSink {
    fn exec_batch(&mut self) -> oracle::Result<()> {
        let mut batch = self
            .conn
            .batch(&self.merge_statement, self.batch_params.len())
            .build()?;
        for params in self.batch_params.drain(..) {
            for (i, (field, typ)) in params
                .params
                .values
                .into_iter()
                .zip(&self.field_types)
                .enumerate()
            {
                batch.set(i + 1, &OraField(field, *typ))?;
            }
            batch.set(self.field_types.len() + 1, &(params.op_kind as u64))?;
            batch.append_row(&[])?;
        }
        batch.execute()?;
        Ok(())
    }

    fn batch(&mut self, kind: OpKind, record: Record) -> oracle::Result<()> {
        self.batch_params.push(BatchedOperation {
            op_kind: kind,
            params: record,
        });
        if self.batch_params.len() >= self.batch_size {
            self.exec_batch()?;
        }
        Ok(())
    }
}

impl Sink for OracleSink {
    fn commit(
        &mut self,
        _epoch_details: &dozer_core::epoch::Epoch,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        // TODO: Make the duraton configurable, and move logic to pipeline
        if self.last_commit.elapsed() > Duration::from_millis(500) {
            self.last_commit = Instant::now();
            self.exec_batch()?;
            self.conn.commit()?;
        }
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: dozer_core::node::PortHandle,
        _record_store: &dozer_recordstore::ProcessorRecordStore,
        op: dozer_types::types::OperationWithId,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        match op.op {
            dozer_types::types::Operation::Delete { old } => {
                self.batch(OpKind::Delete, old)?;
            }
            dozer_types::types::Operation::Insert { new } => {
                self.batch(OpKind::Insert, new)?;
            }
            dozer_types::types::Operation::Update { old, new } => {
                let old_index = old.get_fields_by_indexes(&self.pk);
                let new_index = new.get_fields_by_indexes(&self.pk);
                if old_index != new_index {
                    return Err(Box::new(Error::UpdatedPrimaryKey {
                        old: old_index,
                        new: new_index,
                    }));
                }

                self.batch(OpKind::Update, new)?;
            }
            dozer_types::types::Operation::BatchInsert { mut new } => {
                let mut batch = self
                    .conn
                    .batch(&self.insert_append, self.batch_size)
                    .build()?;
                for record in new.drain(..) {
                    for (i, (field, typ)) in
                        record.values.into_iter().zip(&self.field_types).enumerate()
                    {
                        batch.set(i + 1, &OraField(field, *typ))?;
                    }
                    batch.append_row(&[])?;
                }
                batch.execute()?;
            }
        }
        Ok(())
    }

    fn persist(
        &mut self,
        _epoch: &dozer_core::epoch::Epoch,
        _queue: &dozer_log::storage::Queue,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        // Try to use parallel DML for snapshotting to use direct-path inserts for performance
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        _id: Option<dozer_types::node::OpIdentifier>,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        Ok(())
    }

    fn set_source_state(
        &mut self,
        _source_state: &[u8],
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        Ok(())
    }

    fn get_source_state(
        &mut self,
    ) -> Result<Option<Vec<u8>>, dozer_types::errors::internal::BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(
        &mut self,
    ) -> Result<Option<dozer_types::node::OpIdentifier>, dozer_types::errors::internal::BoxedError>
    {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use dozer_types::types::FieldDefinition;

    use super::*;

    fn trim_str(s: impl AsRef<str>) -> String {
        s.as_ref()
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn test_generate_merge_stmt() {
        let mut schema = Schema::new();
        schema
            .field(f("id"), true)
            .field(f("name"), true)
            .field(f("content"), false);

        let stmt = generate_merge_statement("tablename", &schema);
        assert_eq!(
            trim_str(stmt),
            trim_str(
                r#"
                MERGE INTO "tablename" D 
                USING (SELECT :1 "id", :2 "name", :3 "content", :4 DOZER_OPKIND FROM DUAL) S
                ON (S.DOZER_OPKIND > 0)
                WHEN NOT MATCHED THEN INSERT (D."id", D."name", D."content") VALUES (S."id", S."name", S."content")
                WHEN MATCHED THEN UPDATE SET D."id" = S."id", D."name" = S."name", D."content" = S."content"
                WHERE D."id" = S."id" AND D."name" = S."name"
                DELETE WHERE S.DOZER_OPKIND = 2
"#
            )
        )
    }

    fn f(name: &str) -> FieldDefinition {
        dozer_types::types::FieldDefinition {
            name: name.to_owned(),
            typ: FieldType::String,
            nullable: false,
            source: dozer_types::types::SourceDefinition::Dynamic,
        }
    }
}
