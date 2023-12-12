/// Cache implementation using SQLite
///
/// Every cache consists of a table with the corresponding endpoint's schema,
/// plus a version which is incremented on update. SQLite's `rowid` is used
/// for identity.
///
/// Full-text search indices are implemented as auxiliary [fts5] contentless-delete
/// tables, which are kept up-to-date using triggers on the cache's main table.
/// In queries that can make use of FTS, the query is filtered using a subquery
/// on the corresponding fts table by `rowid`.
///
/// Prepared statements are used as much as possible to avoid query parsing overhead,
/// which can be especially impactful during ingestion. Read query statements are
/// dynamically built based on the requested [QueryExpression], but the statements
/// are LRU-cached to support subsequent queries using the same filters.
/// [fts5]: https://sqlite.org/fts5.html
use std::{cmp::Ordering, iter, path::PathBuf};

use bincode::config::legacy;
use dozer_types::{
    geo::Point,
    json_types::{json_value_to_serde_json, serde_json_to_json_value},
    json_value_to_field,
    log::warn,
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    serde_json,
    types::{
        DozerPoint, Field, FieldDefinition, FieldType, IndexDefinition, Record, Schema,
        SchemaWithIndex,
    },
};
use itertools::Itertools;
use ouroboros::self_referencing;
use r2d2::ManageConnection;
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, ValueRef},
    Connection, OptionalExtension, ParamsFromIter, Row, Statement, ToSql, Transaction,
};
use std::cell::RefCell;

use crate::{
    cache::UpsertResult,
    errors::{CacheError, PlanError},
};

use super::{
    expression::{FilterExpression, QueryExpression, Skip},
    CacheRecord, RecordMeta, RoCache, RwCache,
};

#[derive(Debug)]
struct SqliteConnection {
    path: PathBuf,
}

const DECIMAL_COLLATION: &str = "decimal_collation";
const DECIMAL_COLLATION_PHRASE: &str = " COLLATE decimal_collation";
const VERSION_COL: &str = "__dozer_record_version";

impl ManageConnection for SqliteConnection {
    type Connection = Connection;

    type Error = CacheError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let c = Connection::open(&self.path)?;
        let journal_mode =
            c.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))?;
        if journal_mode.to_lowercase() != "wal" {
            // This should never happen
            warn!("Sqlite WAL journal mode not supported for cache. Concurrency and performance may be suboptimal");
        }
        c.create_collation(DECIMAL_COLLATION, decimal_collation)?;
        Ok(c)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute("SELECT 1", ())?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub struct WriteStatements<'this> {
    insert: Statement<'this>,
    update: Statement<'this>,
    delete: Statement<'this>,
}

#[self_referencing]
pub struct SqliteRwCache {
    name: String,
    schema: SchemaWithIndex,
    // Column name to fts table name
    connection: Connection,
    #[borrows(connection)]
    #[covariant]
    transaction: Option<Transaction<'this>>,
    #[borrows(connection)]
    #[not_covariant]
    get_pk_stmt: RefCell<Statement<'this>>,
    #[borrows(connection)]
    #[covariant]
    write_stmts: WriteStatements<'this>,
}

struct FieldTypeFromSql(FieldType);

fn field_typ_to_string(typ: &FieldType) -> &'static str {
    // sqlite types can be anything, but some tye names cause some column affinities
    // This can be important for numeric types stored as strings, because
    // > For conversions between TEXT and REAL storage classes,
    // > only the first 15 significant decimal digits of the number are preserved.
    // This comes into play for decimal, as that cannot reasonably be stored as
    // a blob with correct lexicographic ordering, so it needs a custom collation sequence.
    match typ {
        FieldType::UInt => "UINT",
        FieldType::U128 => "U128",
        // This is not `INTEGER` by design. If it were, an int column might become
        // a table's rowid if it's the sole primary key, and updates to the column
        // would make the row's identity unstable. To be safe, we don't allow
        // this by making this `INT` instead.
        FieldType::Int => "INT",
        // Using the i128_blob feature, i128 will always have BLOB storage type,
        // so no need for a specific affinity here
        FieldType::I128 => "I128",
        FieldType::Float => "FLOAT",
        FieldType::Boolean => "BOOLEAN",
        // This is "VARCHAR", so that the column affinity is string, instead of numeric
        // This might save some headaches for when strings happen to also represent valid
        // numbers
        FieldType::String => "VARCHAR",
        FieldType::Text => "TEXT",
        FieldType::Binary => "BLOB",
        // This needs to be specifically a string type, otherwise when storing,
        // it might get turned into a real, only keeping the first 15 significant digits.
        // Including "CLOB" will ensure this has String type, which is needed to have a custom
        // collating function
        FieldType::Decimal => "DECIMAL CLOB",
        FieldType::Timestamp => "TIMESTAMP",
        FieldType::Date => "DATE",
        // No need to make this jsonb, as the space savings are usually only about
        // 5-10% and sqlite's jsonb is not yet released as of writing this
        FieldType::Json => "JSON",
        // Ordering on points will be ~undefined, which is fine
        FieldType::Point => "POINT",
        FieldType::Duration => "DURATION",
    }
}

fn field_typ_from_string(string: &str) -> Result<FieldType, rusqlite::types::FromSqlError> {
    let ft = match string.to_uppercase().as_str() {
        "UINT" => FieldType::UInt,
        "U128" => FieldType::U128,
        "INT" => FieldType::Int,
        "I128" => FieldType::I128,
        "FLOAT" => FieldType::Float,
        "BOOLEAN" => FieldType::Boolean,
        "VARCHAR" => FieldType::String,
        "TEXT" => FieldType::Text,
        "BLOB" => FieldType::Binary,
        "DECIMAL CLOB" => FieldType::Decimal,
        "TIMESTAMP" => FieldType::Timestamp,
        "DATE" => FieldType::Date,
        "JSON" => FieldType::Json,
        "POINT" => FieldType::Point,
        "DURATION" => FieldType::Duration,
        s => {
            return Err(rusqlite::types::FromSqlError::Other(
                format!("Invalid data type string \"{s}\"").into(),
            ))
        }
    };
    Ok(ft)
}

impl FromSql for FieldTypeFromSql {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let string = value.as_str()?;
        Ok(Self(field_typ_from_string(string)?))
    }
}

struct SqlField<'a>(&'a Field);

/// Compare decimal values without parsing them
fn decimal_collation(l: &str, r: &str) -> Ordering {
    // A stored decimal is never empty
    let cmp = l.cmp(r);
    // If both decimals are negative, their lexicographical
    // ordering will be reversed
    if l.starts_with('-') && r.starts_with('-') {
        cmp.reverse()
    } else {
        cmp
    }
}

// We only implement ToSql and not FromSql, as the appropriate dozer type depends
// on the schema, not just on the value
impl<'a> ToSql for SqlField<'a> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self.0 {
            Field::UInt(value) => value.to_sql(),
            Field::U128(value) => {
                let bytes = value.to_be_bytes().to_vec();
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(bytes)))
            }
            Field::Int(value) => value.to_sql(),
            Field::I128(value) => value.to_sql(),
            Field::Float(value) => value.to_sql(),
            Field::Boolean(value) => value.to_sql(),
            Field::String(value) => value.to_sql(),
            Field::Text(value) => value.to_sql(),
            Field::Binary(value) => value.to_sql(),
            Field::Decimal(value) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(
                value.to_string(),
            ))),
            Field::Timestamp(value) => value.to_sql(),
            Field::Date(value) => value.to_sql(),
            Field::Json(value) => {
                let v = json_value_to_serde_json(value);
                // Remove reference to `v` created by to_sql
                Ok(match v.to_sql()? {
                    ToSqlOutput::Borrowed(valueref) => ToSqlOutput::Owned(valueref.into()),
                    ToSqlOutput::Owned(value) => ToSqlOutput::Owned(value),
                    _ => unimplemented!(),
                })
            }
            // Ordering is x-then-y
            Field::Point(point) => {
                let (x, y) = point.0.x_y();
                let mut bytes = Vec::with_capacity(16);
                let encode_float = |float: f64| {
                    let mut float = float.to_bits();
                    // if negative
                    if (float & (1 << 63)) != 0 {
                        // invert
                        float ^= u64::MAX
                    } else {
                        // flip the sign bit
                        float ^= 1 << 63
                    }
                    float.to_be_bytes()
                };
                bytes.extend_from_slice(&encode_float(x.0));
                bytes.extend_from_slice(&encode_float(y.0));
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(bytes)))
            }
            // Ordering is undefined (but correct if the resolution matches)
            Field::Duration(value) => {
                let bytes = bincode::encode_to_vec(value, legacy())
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(bytes)))
            }
            Field::Null => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Null)),
        }
    }
}

macro_rules! check_null {
    ($value:expr) => {
        if let Some(v) = $value {
            v
        } else {
            return Ok(Field::Null);
        }
    };
}

fn field_from_sql(value: ValueRef<'_>, typ: &FieldType) -> Result<Field, FromSqlError> {
    let field = match typ {
        FieldType::UInt => Field::UInt(check_null!(FromSql::column_result(value)?)),
        FieldType::U128 => {
            let bytes = check_null!(value.as_bytes_or_null()?);
            let bytes_exact = bytes
                .try_into()
                .map_err(|_| FromSqlError::InvalidBlobSize {
                    expected_size: 8,
                    blob_size: bytes.len(),
                })?;
            Field::U128(u128::from_be_bytes(bytes_exact))
        }
        FieldType::Int => Field::Int(check_null!(FromSql::column_result(value)?)),
        FieldType::I128 => Field::I128(check_null!(FromSql::column_result(value)?)),
        FieldType::Float => Field::Float(OrderedFloat(check_null!(FromSql::column_result(value)?))),
        FieldType::Boolean => Field::Boolean(check_null!(FromSql::column_result(value)?)),
        FieldType::String => Field::String(check_null!(FromSql::column_result(value)?)),
        FieldType::Text => Field::Text(check_null!(FromSql::column_result(value)?)),
        FieldType::Binary => Field::Binary(check_null!(FromSql::column_result(value)?)),
        FieldType::Decimal => Field::Decimal(
            Decimal::from_str_exact(check_null!(value.as_str_or_null()?))
                .map_err(|e| FromSqlError::Other(Box::new(e)))?,
        ),
        FieldType::Timestamp => Field::Timestamp(check_null!(FromSql::column_result(value)?)),
        FieldType::Date => Field::Date(check_null!(FromSql::column_result(value)?)),
        FieldType::Json => {
            let v: serde_json::Value = check_null!(FromSql::column_result(value)?);
            let json = serde_json_to_json_value(v).map_err(|e| FromSqlError::Other(Box::new(e)))?;
            Field::Json(json)
        }
        // Ordering is x-then-y
        FieldType::Point => {
            let bytes = check_null!(value.as_bytes_or_null()?);
            if bytes.len() != 16 {
                return Err(FromSqlError::InvalidBlobSize {
                    expected_size: 16,
                    blob_size: bytes.len(),
                });
            }
            let (x, y) = bytes.split_at(8);
            // We checked the size, so unwrap is safe
            let x = u64::from_be_bytes(x.try_into().unwrap());
            let y = u64::from_be_bytes(y.try_into().unwrap());
            let decode_float = |mut bits: u64| {
                // if negative
                if (bits & (1 << 63)) == 0 {
                    // invert
                    bits ^= u64::MAX
                } else {
                    // flip the sign bit
                    bits ^= 1 << 63
                }
                OrderedFloat(f64::from_bits(bits))
            };
            Field::Point(DozerPoint(Point::new(decode_float(x), decode_float(y))))
        } // Ordering is undefined (but correct if the resolution matches)
        FieldType::Duration => {
            let bytes = check_null!(value.as_bytes_or_null()?);
            let (duration, _) = bincode::decode_from_slice(bytes, legacy())
                .map_err(|e| FromSqlError::Other(Box::new(e)))?;
            Field::Duration(duration)
        }
    };
    Ok(field)
}

enum IndexAction {
    Create(IndexDefinition),
    Remove(String),
}

enum SchemaCheckResult {
    CreateSchema,
    AdjustIndex(Vec<IndexAction>),
    Nothing,
}

impl SqliteRwCache {
    fn create_table(conn: &Connection, name: &str, schema: &Schema) -> Result<(), CacheError> {
        let primary_key_fields = schema
            .primary_index
            .iter()
            .map(|ix| &schema.fields[*ix].name)
            .join(", ");
        let constraints = if !primary_key_fields.is_empty() {
            format!(", PRIMARY KEY ({primary_key_fields})")
        } else {
            // In this case, the table will still have a rowid acting as the primary
            // key.
            String::new()
        };
        let sql = format!(
            "CREATE TABLE {name} ({}{constraints})",
            schema
                .fields
                .iter()
                .map(|field| {
                    let name = &field.name;
                    let typ = field_typ_to_string(&field.typ);
                    let nullable = if field.nullable { "" } else { " NOT NULL" };
                    let collate = if let FieldType::Decimal = field.typ {
                        DECIMAL_COLLATION_PHRASE
                    } else {
                        ""
                    };
                    format!("\"{name}\" {typ}{collate}{nullable}")
                })
                // No need to store id separately, as we already have the rowid
                // XXX: possibly optimize the no-primary-key case, where updates
                // need to look up the entire record.
                .chain([format!("{VERSION_COL} INTEGER NOT NULL DEFAULT 1")])
                .join(", ")
        );
        conn.execute(&sql, ())?;
        Ok(())
    }

    fn try_load_schema(conn: &Connection, name: &str) -> Result<Option<Schema>, rusqlite::Error> {
        // Filter out the version column, as that is not part of the schema
        let mut stmt = conn.prepare(
            "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?) WHERE name != ?",
        )?;
        let cols = stmt.query_map((name, VERSION_COL), |row| {
            let name: String = row.get(0)?;
            let r#type: FieldTypeFromSql = row.get(1)?;
            let nullable: bool = !row.get(2)?;
            let pk: bool = row.get(3)?;
            Ok((
                FieldDefinition::new(
                    name,
                    r#type.0,
                    nullable,
                    dozer_types::types::SourceDefinition::Dynamic,
                ),
                pk,
            ))
        })?;
        let mut schema = Schema::new();
        for field_and_pk in cols {
            let (field, pk) = field_and_pk?;
            schema.field(field, pk);
        }
        if !schema.fields.is_empty() {
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    fn try_load_indices(
        conn: &Connection,
        name: &str,
        schema: &Schema,
    ) -> Result<Vec<(String, IndexDefinition)>, CacheError> {
        let mut stmt =
            conn.prepare("SELECT name FROM pragma_index_list(?) WHERE origin != 'pk'")?;
        let results = stmt.query_map((name,), |row| row.get::<_, String>(0))?;
        let mut index_info_stmt =
            conn.prepare("SELECT cid FROM pragma_index_info(?) ORDER BY seqno")?;
        let mut indices = Vec::new();
        for result in results {
            let index_name = result?;
            let fields = index_info_stmt
                .query_map((&index_name,), |row| row.get::<_, usize>(0))?
                .try_collect()?;
            indices.push((index_name, IndexDefinition::SortedInverted(fields)));
        }

        let mut fts_table_stmt = conn
            .prepare("SELECT name FROM pragma_table_list WHERE type='virtual' AND name LIKE ?")?;
        let mut fts_cols_stmt = conn.prepare("SELECT name FROM pragma_table_info(?)")?;
        for result in
            fts_table_stmt.query_map((format!("fts_{name}_%"),), |row| row.get::<_, String>(0))?
        {
            let tbl_name = result?;
            let col_name: String = fts_cols_stmt.query_row((&tbl_name,), |row| row.get(0))?;
            let (col_idx, _) = schema.get_field_index(&col_name)?;
            indices.push((tbl_name, IndexDefinition::FullText(col_idx)));
        }
        Ok(indices)
    }

    fn create_fts_table_name(name: &str, column: &str) -> String {
        format!("fts_{name}_{column}")
    }

    fn create_index(
        conn: &Connection,
        name: &str,
        index: &IndexDefinition,
        schema: &Schema,
    ) -> Result<(), CacheError> {
        match index {
            IndexDefinition::SortedInverted(fields) => {
                let field_names = fields
                    .iter()
                    .map(|field_ix| &schema.fields[*field_ix].name)
                    .collect_vec();
                let ix_name = format!("ix_{name}_{}", fields.iter().join("_"));
                let sql = format!(
                    "CREATE INDEX {ix_name} ON {name} ({})",
                    field_names.iter().join(", ")
                );
                conn.execute(&sql, ())?;
                Ok(())
            }
            IndexDefinition::FullText(field_idx) => {
                let column_name = &schema.fields[*field_idx].name;
                let fts_tbl_name = Self::create_fts_table_name(name, column_name);
                let sql = format!(
                    "
CREATE VIRTUAL TABLE {fts_tbl_name} USING fts5({column_name}, content='', contentless_delete=1);
CREATE TRIGGER trig_{fts_tbl_name}_insert AFTER INSERT ON {name} BEGIN
    INSERT INTO {fts_tbl_name}(rowid, {column_name}) VALUES (new.rowid, new.{column_name});
END;
CREATE TRIGGER trig_{fts_tbl_name}_update AFTER UPDATE ON {name} BEGIN
    UPDATE {fts_tbl_name} SET {column_name} = new.{column_name} WHERE {fts_tbl_name}.rowid = new.rowid;
END;
CREATE TRIGGER trig_{fts_tbl_name}_delete AFTER DELETE ON {name} BEGIN
    DELETE FROM {fts_tbl_name} WHERE rowid = old.rowid;
END;
                ");
                conn.execute_batch(&sql)?;
                Ok(())
            }
        }
    }

    fn remove_index(conn: &Connection, index: &str) -> Result<(), CacheError> {
        let sql = format!("DROP INDEX {index}");
        conn.execute(&sql, ())?;
        Ok(())
    }

    pub fn open_or_create(
        name: String,
        schema: Option<SchemaWithIndex>,
        connection: Connection,
    ) -> Result<Self, CacheError> {
        let old_schema = Self::try_load_schema(&connection, &name)?;
        let old_indices = if let Some(old_schema) = &old_schema {
            Self::try_load_indices(&connection, &name, old_schema)?
        } else {
            Vec::new()
        };

        let (schema, indices, result) = match (schema, old_schema.map(|s| (s, old_indices))) {
            // Difference in indices can be fixed without rebuilding
            (Some(schema), Some(old_schema)) if old_schema.0 != schema.0 => {
                let old_indices = old_schema.1.into_iter().map(|(_, ix)| ix).collect();
                return Err(CacheError::SchemaMismatch {
                    name,
                    given: Box::new(schema),
                    stored: Box::new((old_schema.0, old_indices)),
                });
            }
            (Some((schema, indices)), Some((_, mut old_indices))) => {
                let mut actions = Vec::new();
                for index in &indices {
                    let position = old_indices.iter().position(|(_, ix)| ix == index);
                    match position {
                        Some(pos) => {
                            old_indices.swap_remove(pos);
                        }
                        None => actions.push(IndexAction::Create(index.clone())),
                    }
                }
                actions.extend(
                    old_indices
                        .into_iter()
                        .map(|(name, _)| IndexAction::Remove(name)),
                );
                (schema, indices, SchemaCheckResult::AdjustIndex(actions))
            }
            (Some((schema, indices)), None) => (schema, indices, SchemaCheckResult::CreateSchema),
            (None, Some((schema, indices))) => (
                schema,
                indices.into_iter().map(|(_, index)| index).collect(),
                SchemaCheckResult::Nothing,
            ),
            (None, None) => {
                return Err(CacheError::SchemaNotFound);
            }
        };

        match result {
            SchemaCheckResult::CreateSchema => {
                Self::create_table(&connection, &name, &schema)?;
                for index in &indices {
                    Self::create_index(&connection, &name, index, &schema)?;
                }
            }
            SchemaCheckResult::AdjustIndex(actions) => {
                // Removals first, so there is no chance of conflicting names
                for removal in actions.iter().filter_map(|action| match action {
                    IndexAction::Remove(ix) => Some(ix),
                    _ => None,
                }) {
                    Self::remove_index(&connection, removal)?;
                }
                for new_index in actions.iter().filter_map(|action| match action {
                    IndexAction::Create(ix) => Some(ix),
                    _ => None,
                }) {
                    Self::create_index(&connection, &name, new_index, &schema)?;
                }
            }
            SchemaCheckResult::Nothing => {}
        }

        let pk_selection = schema
            .primary_index
            .iter()
            .map(|pk| format!(r#""{}" = ?"#, schema.fields[*pk].name))
            .join(" AND ");
        let get_pk_sql = format!(
            "SELECT *, {VERSION_COL}, _rowid_ FROM {name} WHERE {}",
            pk_selection
        );

        let insert_sql = format!(
            "INSERT INTO {name} ({}) VALUES ({})",
            schema
                .fields
                .iter()
                .map(|field| format!("\"{}\"", &field.name))
                .join(","),
            iter::repeat("?").take(schema.fields.len()).join(", ")
        );

        let update_sql = format!(
            "UPDATE {name} SET {}, {VERSION_COL} = {VERSION_COL} + 1 WHERE {pk_selection} RETURNING {VERSION_COL}, _rowid_",
            schema
                .fields
                .iter()
                .map(|field| format!("\"{}\" = ?", &field.name))
                .join(", "),
        );

        let delete_sql =
            format!("DELETE FROM {name} WHERE {pk_selection} RETURNING {VERSION_COL}, _rowid_");

        SqliteRwCacheTryBuilder {
            name,
            schema: (schema, indices),
            connection,
            transaction_builder: |_| -> Result<_, CacheError> { Ok(None) },
            get_pk_stmt_builder: |connection| Ok(RefCell::new(connection.prepare(&get_pk_sql)?)),
            write_stmts_builder: |connection| {
                Ok(WriteStatements {
                    insert: connection.prepare(&insert_sql)?,
                    update: connection.prepare(&update_sql)?,
                    delete: connection.prepare(&delete_sql)?,
                })
            },
        }
        .try_build()
    }

    fn map_row(&self, row: &Row<'_>) -> Result<CacheRecord, rusqlite::Error> {
        let fields = &self.get_schema().0.fields;
        let column_values = fields
            .iter()
            .enumerate()
            .map(|(i, field)| -> Result<Field, rusqlite::Error> {
                Ok(field_from_sql(row.get_ref(i)?, &field.typ)?)
            })
            .try_collect()?;
        let version = row.get(fields.len())?;
        let id = row.get(fields.len() + 1)?;
        Ok(CacheRecord {
            id,
            version,
            record: Record::new(column_values),
        })
    }

    fn map_params<'a>(
        params: impl Iterator<Item = &'a Field> + 'a,
    ) -> ParamsFromIter<impl Iterator<Item = SqlField<'a>> + 'a> {
        rusqlite::params_from_iter(params.map(SqlField))
    }

    fn build_filter(
        &self,
        filter: &FilterExpression,
        params: &mut Vec<Field>,
    ) -> Result<String, PlanError> {
        match filter {
            super::expression::FilterExpression::Simple(col, op, value) => {
                let (col_idx, def) = self.borrow_schema().0.get_field_index(col)?;
                let mut v = json_value_to_field(value.clone(), def.typ, def.nullable)?;
                let predicate = match op {
                    super::expression::Operator::LT => format!("{col} < ?"),
                    super::expression::Operator::LTE => format!("{col} <= ?"),
                    super::expression::Operator::EQ => format!("{col} = ?"),
                    super::expression::Operator::GT => format!("{col} > ?"),
                    super::expression::Operator::GTE => format!("{col} >= ?"),
                    super::expression::Operator::MatchesAny => todo!(),
                    super::expression::Operator::MatchesAll => todo!(),
                    super::expression::Operator::Contains => {
                        if self
                            .get_schema()
                            .1
                            .contains(&IndexDefinition::FullText(col_idx))
                        {
                            // quote double-quotes in the value and make the string a phrase by placing
                            // double-quotes around it
                            let param = format!("\"{}\"", v.to_string().replace('"', "\"\""));
                            v = Field::String(param);
                            // Create a subquery
                            format!(
                                "_rowid_ IN (SELECT _rowid_ from {} WHERE {col} MATCH ?)",
                                Self::create_fts_table_name(self.borrow_name(), col)
                            )
                        } else {
                            format!("{col} LIKE '%' + ? + '%'")
                        }
                    }
                };
                params.push(v);
                Ok(predicate)
            }
            super::expression::FilterExpression::And(filters) => {
                let parts: Vec<String> = filters
                    .iter()
                    .map(|filter| self.build_filter(filter, params))
                    .try_collect()?;
                Ok(format!("({})", parts.join(" AND ")))
            }
        }
    }

    fn build_query(&self, expr: &QueryExpression) -> Result<(String, Vec<Field>), CacheError> {
        let mut params = Vec::new();
        let mut filters_clause = match &expr.filter {
            Some(filter) => self.build_filter(filter, &mut params)?,
            None => "1".to_string(),
        };
        if let Skip::After(after) = expr.skip {
            filters_clause.push_str(" AND _rowid_ = ?");
            params.push(Field::UInt(after));
        }
        let order_exprs = &expr.order_by.0;
        let order_expr = if !order_exprs.is_empty() {
            // Check that all exist
            for order in order_exprs {
                self.borrow_schema().0.get_field_index(&order.field_name)?;
            }
            order_exprs
                .iter()
                .map(|field| {
                    let column = &field.field_name;
                    let direction = match field.direction {
                        crate::cache::expression::SortDirection::Ascending => "ASC",
                        crate::cache::expression::SortDirection::Descending => "DESC",
                    };
                    format!("{column} {direction}",)
                })
                .join(", ")
        } else {
            "rowid".to_string()
        };

        params.push(Field::Int(expr.limit.map_or(-1i64, |limit| limit as i64)));
        if let Skip::Skip(skip) = expr.skip {
            params.push(Field::UInt(skip as u64));
        } else {
            params.push(Field::UInt(0));
        };
        let table = &self.borrow_name();
        let query = format!("SELECT *, _rowid_ FROM {table} WHERE {filters_clause} ORDER BY {order_expr} LIMIT ? OFFSET ?");
        Ok((query, params))
    }

    fn transaction(&mut self) -> Result<&Transaction<'_>, CacheError> {
        self.with_mut(|fields| -> Result<(), CacheError> {
            if fields.transaction.is_none() {
                // Unchecked transaction is needed, because of the self-referential
                // nature of storing the transaction with the connection. This
                // is sound at a database level, because we guarantee only one
                // transaction is active at any time using the `Option`.
                let t = fields.connection.unchecked_transaction()?;
                *fields.transaction = Some(t);
            }
            Ok(())
        })?;
        Ok(self.borrow_transaction().as_ref().unwrap())
    }

    fn get_pk<'a>(schema: &'a Schema, record: &'a Record) -> impl Iterator<Item = &'a Field> + 'a {
        let index = &schema.primary_index;
        index.iter().map(|pk| record.get_value(*pk).unwrap())
    }
}

impl RoCache for SqliteRwCache {
    fn name(&self) -> &str {
        self.borrow_name()
    }

    fn labels(&self) -> &dozer_tracing::Labels {
        todo!()
    }

    fn get_schema(&self) -> &dozer_types::types::SchemaWithIndex {
        self.borrow_schema()
    }

    fn get(&self, key: &[Field]) -> Result<super::CacheRecord, crate::errors::CacheError> {
        self.with_get_pk_stmt(|stmt| {
            Ok(stmt
                .borrow_mut()
                .query_row(Self::map_params(key.iter()), |r| self.map_row(r))?)
        })
    }

    fn count(&self, query: &QueryExpression) -> Result<usize, crate::errors::CacheError> {
        let (sql, params) = self.build_query(query)?;
        let sql = format!("SELECT COUNT(*) FROM ({sql})");
        let mut stmt = self.borrow_connection().prepare_cached(&sql)?;
        Ok(stmt.query_row(Self::map_params(params.iter()), |row| row.get(0))?)
    }

    fn query(
        &self,
        query: &QueryExpression,
    ) -> Result<Vec<super::CacheRecord>, crate::errors::CacheError> {
        let (sql, params) = self.build_query(query)?;
        let mut stmt = self.borrow_connection().prepare_cached(&sql)?;
        let rows = stmt
            .query_map(Self::map_params(params.iter()), |row| self.map_row(row))?
            .try_collect()?;
        Ok(rows)
    }

    fn get_commit_state(&self) -> Result<Option<super::CommitState>, crate::errors::CacheError> {
        // TODO: Properly implement commit state management
        Ok(None)
    }

    fn is_snapshotting_done(&self) -> Result<bool, crate::errors::CacheError> {
        todo!()
    }
}

impl RwCache for SqliteRwCache {
    fn insert(
        &mut self,
        record: &dozer_types::types::Record,
    ) -> Result<super::UpsertResult, CacheError> {
        let _ = self.transaction()?;
        let rowid = self.with_write_stmts_mut(|stmts| {
            stmts.insert.insert(Self::map_params(record.values.iter()))
        })?;
        Ok(super::UpsertResult::Inserted {
            meta: super::RecordMeta {
                id: rowid as u64,
                version: 1,
            },
        })
    }

    fn delete(
        &mut self,
        record: &dozer_types::types::Record,
    ) -> Result<Option<super::RecordMeta>, CacheError> {
        let _ = self.transaction()?;
        let meta = self.with_mut(|fields| {
            let pk = Self::get_pk(&fields.schema.0, record);
            fields
                .write_stmts
                .delete
                .query_row(Self::map_params(pk), |row| {
                    let version = row.get(0)?;
                    let id = row.get(1)?;
                    Ok(RecordMeta { id, version })
                })
                .optional()
        })?;
        Ok(meta)
    }

    fn update(
        &mut self,
        old: &dozer_types::types::Record,
        record: &dozer_types::types::Record,
    ) -> Result<super::UpsertResult, CacheError> {
        let _ = self.transaction()?;
        let (old_meta, new_meta) = self.with_mut(|fields| {
            let pk = Self::get_pk(&fields.schema.0, old);
            let params = record.values.iter().chain(pk);
            fields
                .write_stmts
                .update
                .query_row(Self::map_params(params), |row| {
                    let new_version = row.get(0)?;
                    let id = row.get(1)?;
                    Ok((
                        RecordMeta {
                            id,
                            version: new_version - 1,
                        },
                        RecordMeta {
                            id,
                            version: new_version,
                        },
                    ))
                })
        })?;
        Ok(UpsertResult::Updated { old_meta, new_meta })
    }

    fn set_connection_snapshotting_done(
        &mut self,
        _connection_name: &str,
    ) -> Result<(), CacheError> {
        todo!()
    }

    fn commit(&mut self, _state: &super::CommitState) -> Result<(), CacheError> {
        self.with_mut(|fields| {
            if let Some(t) = fields.transaction.take() {
                // TODO: Properly implement commit state management
                t.commit()?;
            }
            Ok(())
        })
    }

    fn as_ro(&self) -> &dyn RoCache {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{
        expression::Operator,
        test_utils::{self, schema_0, schema_1, schema_full_text, schema_multi_indices},
    };

    use super::*;

    #[test]
    fn test_write_read() {
        let schema = test_utils::schema_0();
        let connection = Connection::open_in_memory().unwrap();
        let mut cache =
            SqliteRwCache::open_or_create("test_cache".to_owned(), Some(schema), connection)
                .unwrap();
        let record0 = Record::new(vec![Field::String("record0".to_owned())]);
        cache.insert(&record0).unwrap();
        let record1 = Record::new(vec![Field::String("record1".to_owned())]);
        cache.insert(&record1).unwrap();
        let record2 = Record::new(vec![Field::String("record2".to_owned())]);
        let result = cache.get(&[Field::String("record0".to_owned())]).unwrap();
        assert_eq!(result.version, 1);
        assert_eq!(result.record, record0);
        let record0_id = result.id;
        let result = cache.get(&[Field::String("record1".to_owned())]).unwrap();
        assert_eq!(result.version, 1);
        assert_eq!(&result.record, &record1);

        let count = cache.count(&QueryExpression::with_no_limit()).unwrap();
        assert_eq!(count, 2);
        let count = cache.count(&QueryExpression::with_limit(1)).unwrap();
        assert_eq!(count, 1);

        let query_records: Vec<_> = cache
            .query(&QueryExpression::with_no_limit())
            .unwrap()
            .into_iter()
            .map(|cache_record| cache_record.record)
            .collect();
        assert_eq!(query_records, vec![record0.clone(), record1.clone()]);

        cache.update(&record1, &record2).unwrap();
        let result = cache.get(&[Field::String("record2".to_owned())]).unwrap();
        assert_eq!(result.version, 2);
        assert_eq!(result.record, record2);

        let meta = cache.delete(&record0).unwrap().unwrap();
        assert_eq!(meta.id, record0_id);

        assert!(cache.get(&[Field::String("record0".to_owned())]).is_err());
    }

    fn check_schema(mut schema: SchemaWithIndex) {
        let name = "test".to_owned();
        let conn = Connection::open_in_memory().unwrap();
        let cache =
            SqliteRwCache::open_or_create(name.clone(), Some(schema.clone()), conn).unwrap();
        let conn = cache.borrow_connection();
        let loaded_schema = SqliteRwCache::try_load_schema(conn, &name)
            .unwrap()
            .unwrap();
        assert_eq!(loaded_schema, schema.0);
        let mut loaded_indices = SqliteRwCache::try_load_indices(conn, &name, &loaded_schema)
            .unwrap()
            .into_iter()
            .map(|(_, idx)| idx)
            .collect_vec();
        let sort_index = |left: &IndexDefinition, right: &IndexDefinition| match (left, right) {
            (IndexDefinition::SortedInverted(cols_l), IndexDefinition::SortedInverted(cols_r)) => {
                cols_l.cmp(cols_r)
            }
            (IndexDefinition::SortedInverted(_), IndexDefinition::FullText(_)) => Ordering::Less,
            (IndexDefinition::FullText(_), IndexDefinition::SortedInverted(_)) => Ordering::Greater,
            (IndexDefinition::FullText(col_l), IndexDefinition::FullText(col_r)) => {
                col_l.cmp(col_r)
            }
        };

        schema.1.sort_by(sort_index);
        loaded_indices.sort_by(sort_index);
        assert_eq!(loaded_indices, schema.1);
    }

    #[test]
    fn test_load_schema() {
        check_schema(schema_0());
        check_schema(schema_1());
        check_schema(schema_full_text());
        check_schema(schema_multi_indices());
    }

    #[test]
    fn test_fts_query() {
        let schema = schema_full_text();
        let conn = Connection::open_in_memory().unwrap();
        let mut cache =
            SqliteRwCache::open_or_create("test".to_owned(), Some(schema), conn).unwrap();
        let text = "The quick brown fox jumps over the lazy dog".to_owned();
        let record = Record::new(vec![Field::String(text.clone()), Field::Text(text.clone())]);
        cache.insert(&record).unwrap();

        let mut query = QueryExpression::with_no_limit();
        // Substring search, positive case
        query.filter = Some(FilterExpression::Simple(
            "foo".into(),
            Operator::Contains,
            serde_json::Value::String("brown fox".into()),
        ));

        assert_eq!(
            cache.query(&query).unwrap(),
            vec![CacheRecord {
                id: 1,
                version: 1,
                record,
            }]
        );

        // Substring search, negative case
        let mut query = QueryExpression::with_no_limit();
        query.filter = Some(FilterExpression::Simple(
            "foo".into(),
            Operator::Contains,
            serde_json::Value::String("quick fox".into()),
        ));

        assert_eq!(cache.query(&query).unwrap(), vec![])
    }
}
