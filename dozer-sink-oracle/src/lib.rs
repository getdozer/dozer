use std::collections::HashMap;

use dozer_core::{
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    chrono::{self, DateTime, NaiveDate, Utc},
    errors::internal::BoxedError,
    log::info,
    models::ingestion_types::OracleConfig,
    tonic::async_trait,
    types::{Field, FieldType, Schema},
};
use oracle::{
    sql_type::{OracleType, ToSql},
    Connection,
};

#[derive(Debug)]
struct OracleSink {
    conn: Connection,
    insert_append: String,
    pk: Vec<usize>,
    field_types: Vec<FieldType>,
    insert: String,
    update: String,
    delete: String,
}

#[derive(Debug)]
pub struct OracleSinkFactory {
    pub config: OracleConfig,
    pub table: String,
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
        let insert = format!(
            "INSERT INTO \"{table_name}\" VALUES ({})",
            (1..=schema.fields.len())
                .map(|i| format!(":{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let insert_append = format!(
            "INSERT /*+ APPEND */ INTO \"{table_name}\" VALUES ({})",
            (1..=schema.fields.len())
                .map(|i| format!(":{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let pk_fields = schema
            .primary_index
            .iter()
            .map(|ix| &schema.fields[*ix].name)
            .collect::<Vec<_>>();
        let pk_select = |start_idx: usize| {
            pk_fields
                .iter()
                .enumerate()
                .map(|(i, field)| format!("\"{field}\" = :{}", start_idx + i))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        let update = format!(
            "UPDATE \"{table_name}\" SET {} WHERE {}",
            schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| { format!("\"{}\" = :{}", field.name, i) })
                .collect::<Vec<_>>()
                .join(", "),
            pk_select(schema.fields.len()),
        );
        let delete = format!("DELETE FROM \"{table_name}\" WHERE {}", pk_select(1));

        let field_types = schema.fields.into_iter().map(|field| field.typ).collect();
        Ok(Box::new(OracleSink {
            conn: connection,
            insert_append,
            insert,
            update,
            delete,
            field_types,
            pk: schema.primary_index,
        }))
    }
}

struct OraField(Field, FieldType);
impl OraField {
    fn new(f: Field, typ: FieldType) -> Self {
        Self(f, typ)
    }
}

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

impl Sink for OracleSink {
    fn commit(
        &mut self,
        _epoch_details: &dozer_core::epoch::Epoch,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: dozer_core::node::PortHandle,
        op: dozer_types::types::OperationWithId,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        match op.op {
            dozer_types::types::Operation::Delete { old } => {
                let fields = old
                    .values
                    .into_iter()
                    .zip(&self.field_types)
                    .enumerate()
                    .filter_map(|(i, v)| self.pk.contains(&i).then_some(v))
                    .map(|(f, t)| OraField::new(f, *t))
                    .collect::<Vec<_>>();
                self.conn
                    .statement(&self.delete)
                    .build()?
                    .execute(&fields.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>())?;
                self.conn.commit()?;
            }
            dozer_types::types::Operation::Insert { new } => {
                let fields = new
                    .values
                    .into_iter()
                    .zip(&self.field_types)
                    .map(|(f, t)| OraField::new(f, *t))
                    .collect::<Vec<_>>();
                self.conn
                    .statement(&self.insert)
                    .build()?
                    .execute(&fields.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>())?;
                self.conn.commit()?;
            }
            dozer_types::types::Operation::Update { old, new } => {
                let fields = new
                    .values
                    .into_iter()
                    .zip(&self.field_types)
                    .map(|(f, t)| OraField::new(f, *t))
                    .collect::<Vec<_>>();

                let pk = old
                    .values
                    .iter()
                    .zip(&self.field_types)
                    .enumerate()
                    .filter_map(|(i, v)| {
                        self.pk
                            .contains(&i)
                            .then_some(OraField::new(v.0.clone(), *v.1))
                    })
                    .collect::<Vec<_>>();
                let params = fields
                    .iter()
                    .chain(pk.iter())
                    .map(|v| v as &dyn ToSql)
                    .collect::<Vec<_>>();
                self.conn
                    .statement(&self.update)
                    .build()?
                    .execute(&params)?;
                self.conn.commit()?;
            }
            dozer_types::types::Operation::BatchInsert { new } => {
                let mut batch = self.conn.batch(&self.insert_append, 1000).build()?;
                for record in new {
                    let fields = record
                        .values
                        .into_iter()
                        .zip(&self.field_types)
                        .map(|(f, t)| OraField::new(f, *t))
                        .collect::<Vec<_>>();
                    let params = fields.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();
                    batch.append_row(&params)?;
                }
                batch.execute()?;
                self.conn.commit()?;
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
