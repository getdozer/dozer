use dozer_types::thiserror;
use std::collections::HashMap;

use dozer_core::{
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    chrono::{self, DateTime, NaiveDate, Utc},
    errors::internal::BoxedError,
    log::{debug, info},
    models::ingestion_types::OracleConfig,
    node::OpIdentifier,
    thiserror::Error,
    tonic::async_trait,
    types::{Field, FieldType, Record, Schema},
};
use oracle::{
    sql_type::{OracleType, ToSql},
    Connection,
};

const TXN_ID_COL: &str = "__txn_id";
const TXN_SEQ_COL: &str = "__txn_seq";
const METADATA_TABLE: &str = "__replication_metadata";
const META_TXN_ID_COL: &str = "txn_id";
const META_TABLE_COL: &str = "table";

fn format_null(nullable: bool) -> &'static str {
    if nullable {
        "NULL"
    } else {
        "NOT NULL"
    }
}

#[derive(Error, Debug)]
enum SchemaValidationError {
    #[error("Missing column: {0}")]
    MissingColumn(String),
    #[error("Extra column found: {0:?}")]
    ExtraColumns(Vec<String>),
    #[error("Incompatible type for field {field}. Internal type: {dozer_type}, sink type: {remote_type}")]
    IncompatibleType {
        field: String,
        dozer_type: FieldType,
        remote_type: OracleType,
    },
    #[error("Unsupported type in sink table: {0}")]
    UnsupportedType(String),
    #[error("Incompatibly mismatched nullability. Source: {}, sink: {}", format_null(*.src), format_null(*.sink))]
    MismatchedNullability { src: bool, sink: bool },
}

#[derive(Error, Debug)]
enum Error {
    #[error("Updating a primary key is not supported. Old: {old:?}, new: {new:?}")]
    UpdatedPrimaryKey { old: Vec<Field>, new: Vec<Field> },
    #[error("Destination table {table} has incompatible schema. {inner}")]
    IncompatibleSchema {
        table: String,
        inner: SchemaValidationError,
    },
    #[error("Oracle database error: {0}")]
    Oracle(oracle::Error),
}

impl From<oracle::Error> for Error {
    fn from(value: oracle::Error) -> Self {
        Error::Oracle(value)
    }
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
    insert_metadata: String,
    update_metadata: String,
    select_metadata: String,
    latest_txid: Option<u64>,
}

#[derive(Debug)]
pub struct OracleSinkFactory {
    pub config: OracleConfig,
    pub table: String,
}

fn parse_oracle_type(
    name: &str,
    length: u32,
    precision: Option<u8>,
    scale: Option<i8>,
) -> Option<OracleType> {
    let typ = match name {
        "VARCHAR2" => OracleType::Varchar2(length),
        "NVARCHAR2" => OracleType::NVarchar2(length),
        "CHAR" => OracleType::Char(length),
        "NCHAR" => OracleType::NChar(length),
        "ROWID" => OracleType::Rowid,
        "RAW" => OracleType::Raw(length),
        "BINARY_FLOAT" => OracleType::BinaryFloat,
        "BINARY_DOUBLE" => OracleType::BinaryDouble,
        "NUMBER" => OracleType::Number(precision.unwrap_or(38), scale.unwrap_or(0)),
        "FLOAT" => OracleType::Float(precision.unwrap_or(126)),
        "DATE" => OracleType::Date,
        "JSON" => OracleType::Json,
        _ if name.starts_with("TIMESTAMP") => {
            let fracp_start = name.find('(').unwrap();
            let fracp_end = name.find(')').unwrap();
            let fracp = name[fracp_start + 1..fracp_end].parse().unwrap();

            match &name[fracp_end + 1..] {
                " WITH LOCAL TIME ZONE" => OracleType::TimestampLTZ(fracp),
                " WITH TIME ZONE" => OracleType::TimestampTZ(fracp),
                "" => OracleType::Timestamp(fracp),
                _ => return None,
            }
        }
        _ if name.starts_with("INTERVAL") => {
            if name[9..].starts_with("DAY") {
                let dayp_start = name.find('(').unwrap();
                let dayp_end = name.find(')').unwrap();
                let dayp = name[dayp_start + 1..dayp_end].parse().unwrap();

                let secondp_start = name[dayp_end + 1..].find('(').unwrap();
                let secondp_end = name[dayp_end + 1..].find(')').unwrap();
                let secondp = name[dayp_end + 1..][secondp_start + 1..secondp_end]
                    .parse()
                    .unwrap();
                OracleType::IntervalDS(dayp, secondp)
            } else if name[9..].starts_with("YEAR") {
                let yearp_start = name.find('(').unwrap();
                let yearp_end = name.find(')').unwrap();
                let yearp = name[yearp_start + 1..yearp_end].parse().unwrap();
                OracleType::IntervalYM(yearp)
            } else {
                return None;
            }
        }
        _ => {
            return None;
        }
    };
    Some(typ)
}

impl OracleSinkFactory {
    fn validate_table(
        &self,
        connection: &Connection,
        table_name: &str,
        schema: &Schema,
    ) -> Result<bool, Error> {
        let err = |e| Error::IncompatibleSchema {
            table: table_name.to_owned(),
            inner: e,
        };

        let results = connection.query_as::<(String, String, u32, Option<u8>, Option<i8>, String)>(
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE FROM ALL_TAB_COLS WHERE table_name = :1",
            &[&table_name],
        )?;

        let mut cols = HashMap::new();
        for col in results {
            let col = col?;
            cols.insert(col.0.clone(), col);
        }

        // The table does not exist
        if cols.is_empty() {
            return Ok(false);
        }

        for field in &schema.fields {
            let definition = cols
                .remove(&field.name)
                .ok_or_else(|| err(SchemaValidationError::MissingColumn(field.name.clone())))?;
            let (_, type_name, length, precision, scale, nullable) = definition;
            let Some(typ) = parse_oracle_type(&type_name, length, precision, scale) else {
                return Err(err(SchemaValidationError::UnsupportedType(
                    type_name.clone(),
                )));
            };
            match (field.typ, typ) {
                (
                    FieldType::String | FieldType::Text,
                    OracleType::Varchar2(_) | OracleType::NVarchar2(_),
                ) => {}
                (FieldType::U128 | FieldType::I128, OracleType::Number(precision, 0))
                    if precision >= 20 => {}
                (FieldType::UInt | FieldType::Int, OracleType::Number(precision, 0))
                    if precision < 20 => {}
                (FieldType::Float, OracleType::Number(38, 0) | OracleType::BinaryDouble) => {}
                (FieldType::Boolean, OracleType::Number(_, 0)) => {}
                (FieldType::Binary, OracleType::Raw(_)) => {}
                (FieldType::Timestamp, OracleType::TimestampTZ(_)) => {}
                (FieldType::Date, OracleType::Timestamp(_)) => {}
                (dozer_type, remote_type) => {
                    return Err(err(SchemaValidationError::IncompatibleType {
                        field: field.name.clone(),
                        dozer_type,
                        remote_type,
                    }))
                }
            }
            if (field.nullable, nullable.as_str()) == (true, "N") {
                return Err(err(SchemaValidationError::MismatchedNullability {
                    src: field.nullable,
                    sink: false,
                }));
            }
        }

        if !cols.is_empty() {
            return Err(err(SchemaValidationError::ExtraColumns(
                cols.keys().cloned().collect(),
            )));
        }
        Ok(true)
    }

    fn validate_or_create_table(
        &self,
        connection: &Connection,
        table_name: &str,
        schema: &Schema,
    ) -> Result<(), Error> {
        if self.validate_table(connection, table_name, schema)? {
            return Ok(());
        }

        let mut column_defs = Vec::with_capacity(schema.fields.len() + 2);
        for field in &schema.fields {
            let name = &field.name;
            let col_type = match field.typ {
                dozer_types::types::FieldType::UInt => "NUMBER(19)",
                dozer_types::types::FieldType::U128 => "INTEGER",
                dozer_types::types::FieldType::Int => "NUMBER(19)",
                dozer_types::types::FieldType::I128 => "INTEGER",
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

        Ok(())
    }
}

fn generate_merge_statement(table_name: &str, schema: &Schema) -> String {
    let field_names = schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .chain([TXN_ID_COL, TXN_SEQ_COL]);

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
        .enumerate()
        .filter(|(i, _)| !schema.primary_index.contains(i))
        .map(|(_, name)| format!("D.\"{name}\" = S.\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let mut pk_select = schema
        .primary_index
        .iter()
        .map(|ix| &schema.fields[*ix].name)
        .map(|name| format!("D.\"{name}\" = S.\"{name}\""))
        .collect::<Vec<_>>()
        .join(" AND ");
    if pk_select.is_empty() {
        pk_select = "1 = 1".to_owned();
    }

    let opkind_idx = parameter_index.next().unwrap();

    // Match on PK and txn_id.
    // If the record does not exist and the op is INSERT, do the INSERT
    // If the record exists, but the txid is higher than the operation's txid,
    // do nothing (if the op is INSERT,
    format!(
        r#"MERGE INTO "{table_name}" D
        USING (SELECT {input_fields}, :{opkind_idx} DOZER_OPKIND FROM DUAL) S
        ON (S.DOZER_OPKIND > 0)
        WHEN NOT MATCHED THEN INSERT ({destination_columns}) VALUES ({source_values})
        WHEN MATCHED THEN UPDATE SET {destination_assign} WHERE {pk_select}
        DELETE WHERE S.DOZER_OPKIND = 2"#
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

    fn get_input_port_name(&self, _port: &PortHandle) -> String {
        self.table.clone()
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn build(
        &self,
        mut input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn dozer_core::node::Sink>, BoxedError> {
        let config = &self.config;
        let root_connect_string = format!(
            "{}:{}/{}",
            config.host,
            config.port,
            config.pdb.as_ref().unwrap_or(&config.sid)
        );
        let connection = Connection::connect(&config.user, &config.password, root_connect_string)?;

        let schema = input_schemas.remove(&DEFAULT_PORT_HANDLE).unwrap();

        let table_name = &self.table;

        self.validate_or_create_table(&connection, table_name, &schema)?;
        self.validate_or_create_table(
            &connection,
            METADATA_TABLE,
            Schema::new()
                .field(
                    dozer_types::types::FieldDefinition {
                        name: META_TABLE_COL.to_owned(),
                        typ: FieldType::String,
                        nullable: false,
                        source: dozer_types::types::SourceDefinition::Dynamic,
                    },
                    true,
                )
                .field(
                    dozer_types::types::FieldDefinition {
                        name: META_TXN_ID_COL.to_owned(),
                        typ: FieldType::UInt,
                        nullable: false,
                        source: dozer_types::types::SourceDefinition::Dynamic,
                    },
                    false,
                ),
        )?;

        let insert_append = format!(
            //"INSERT /*+ APPEND */ INTO \"{table_name}\" VALUES ({})",
            "INSERT INTO \"{table_name}\" VALUES ({})",
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
            batch_size: 10000,
            insert_metadata: format!("INSERT INTO \"{METADATA_TABLE}\" (\"{META_TABLE_COL}\", \"{META_TXN_ID_COL}\") VALUES (q'\"{table_name}\"', :1)"),
            update_metadata: format!("UPDATE \"{METADATA_TABLE}\" SET \"{META_TXN_ID_COL}\" = :1 WHERE \"{META_TABLE_COL}\" = q'\"{table_name}\"'") ,
            select_metadata: format!("SELECT \"{META_TXN_ID_COL}\" FROM \"{METADATA_TABLE}\" WHERE \"{META_TABLE_COL}\" = q'\"{table_name}\"'"),
            latest_txid: None,
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
        debug!("Executing batch of size {}", self.batch_params.len());
        let mut batch = self
            .conn
            .batch(&self.merge_statement, self.batch_params.len())
            .build()?;
        for params in self.batch_params.drain(..) {
            let mut bind_idx = 1..;
            for ((field, typ), i) in params
                .params
                .values
                .into_iter()
                .zip(&self.field_types)
                .zip(&mut bind_idx)
            {
                batch.set(i, &OraField(field, *typ))?;
            }
            batch.set(bind_idx.next().unwrap(), &(params.op_kind as u64))?;
            batch.append_row(&[])?;
        }
        batch.execute()?;
        Ok(())
    }

    fn batch(
        &mut self,
        _op_id: Option<OpIdentifier>,
        kind: OpKind,
        record: Record,
    ) -> oracle::Result<()> {
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
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<(), BoxedError> {
        self.exec_batch()?;
        if let Some(txid) = self.latest_txid {
            // If the row_count == 0, we need to insert instead.
            if self
                .conn
                .execute(&self.update_metadata, &[&txid])?
                .row_count()?
                == 0
            {
                self.conn.execute(&self.insert_metadata, &[&txid])?;
            }
        }
        self.conn.commit()?;
        Ok(())
    }

    fn process(
        &mut self,
        op: dozer_types::types::TableOperation,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        self.latest_txid = op.id.map(|id| id.txid);
        match op.op {
            dozer_types::types::Operation::Delete { old } => {
                self.batch(op.id, OpKind::Delete, old)?;
            }
            dozer_types::types::Operation::Insert { new } => {
                self.batch(op.id, OpKind::Insert, new)?;
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

                self.batch(op.id, OpKind::Update, new)?;
            }
            dozer_types::types::Operation::BatchInsert { mut new } => {
                let mut batch = self
                    .conn
                    .batch(&self.insert_append, self.batch_size)
                    .build()?;
                for record in new.drain(..) {
                    let mut bind_idx = 1..;
                    for ((field, typ), i) in record
                        .values
                        .into_iter()
                        .zip(&self.field_types)
                        .zip(&mut bind_idx)
                    {
                        batch.set(i, &OraField(field, *typ))?;
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
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        id: Option<dozer_types::node::OpIdentifier>,
    ) -> Result<(), dozer_types::errors::internal::BoxedError> {
        self.latest_txid = id.map(|opid| opid.txid);
        self.flush_batch()?;
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
        match self.conn.query_row_as::<u64>(&self.select_metadata, &[]) {
            Ok(txid) => Ok(Some(OpIdentifier { txid, seq_in_tx: 0 })),
            Err(oracle::Error::NoDataFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
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
                USING (SELECT :1 "id", :2 "name", :3 "content", :4 "__txn_id", :5 "__txn_seq", :6 DOZER_OPKIND FROM DUAL) S
                ON (D."id" = S."id" AND D."name" = S."name")
                WHEN NOT MATCHED THEN INSERT (D."id", D."name", D."content", D."__txn_id", D."__txn_seq") VALUES (S."id", S."name", S."content", S."__txn_id", S."__txn_seq") WHERE S.DOZER_OPKIND = 0
                WHEN MATCHED THEN UPDATE SET D."content" = S."content", D."__txn_id" = S."__txn_id", D."__txn_seq" = S."__txn_seq"
                WHERE S.DOZER_OPKIND = 1 AND (D."__txn_id" IS NULL
                    OR S."__txn_id" > D."__txn_id"
                    OR (S."__txn_id" = D."__txn_id" AND S."__txn_seq" > D."__txn_seq"))
                DELETE WHERE S.DOZER_OPKIND = 2 AND (D."__txn_id" IS NULL
                    OR S."__txn_id" > D."__txn_id"
                    OR (S."__txn_id" = D."__txn_id" AND S."__txn_seq" > D."__txn_seq"))
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
