use crate::{
    connection::is_network_failure, conversion::get_field_type_for_sql_type, schema::SchemaHelper,
    MySQLConnectorError,
};

use super::{
    connection::Conn,
    conversion::{IntoField, IntoFields, IntoJsonValue},
    schema::{ColumnDefinition, TableDefinition},
};
use dozer_ingestion_connector::{
    dozer_types::{
        json_types::{JsonArray, JsonObject, JsonValue},
        log::{trace, warn},
        models::ingestion_types::IngestionMessage,
        types::Field,
        types::{FieldType, Operation, Record},
    },
    futures::StreamExt,
    Ingestor,
};
use mysql_async::{binlog::EventFlags, BinlogStream, Pool};
use mysql_common::{
    binlog::{
        self,
        events::{RowsEventRows, TableMapEvent},
        jsonb::{Array, ComplexValue, Object, StorageFormat},
        row::BinlogRow,
        value::BinlogValue,
    },
    Row,
};
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

#[derive(Debug, Clone)]
pub struct BinlogPosition {
    pub filename: Vec<u8>,
    pub position: u64,
}

pub async fn get_master_binlog_position(
    conn: &mut Conn,
) -> Result<BinlogPosition, MySQLConnectorError> {
    let (filename, position) = {
        let mut row: Row = conn
            .exec_first("SHOW MASTER STATUS", ())
            .await
            .map_err(MySQLConnectorError::QueryExecutionError)?
            .unwrap();
        (row.take(0).unwrap(), row.take(1).unwrap())
    };

    Ok(BinlogPosition { filename, position })
}

pub async fn get_binlog_format(conn: &mut Conn) -> Result<String, MySQLConnectorError> {
    let mut row: Row = conn
        .exec_first("SELECT @@binlog_format", ())
        .await
        .map_err(MySQLConnectorError::QueryExecutionError)?
        .unwrap();
    let binlog_logging_format = row.take(0).unwrap();
    Ok(binlog_logging_format)
}

pub struct BinlogIngestor<'a, 'd, 'e> {
    ingestor: &'a Ingestor,
    binlog_stream: Option<BinlogStream>,
    next_position: BinlogPosition,
    stop_position: Option<BinlogPosition>,
    local_stop_position: Option<u64>,
    server_id: u32,
    conn_pool: &'d Pool,
    conn_url: &'e String,
}

impl<'a, 'd, 'e> BinlogIngestor<'a, 'd, 'e> {
    pub fn new(
        ingestor: &'a Ingestor,
        start_position: BinlogPosition,
        stop_position: Option<BinlogPosition>,
        server_id: u32,
        (conn_pool, conn_url): (&'d Pool, &'e String),
    ) -> Self {
        Self {
            ingestor,
            binlog_stream: None,
            next_position: start_position,
            stop_position,
            local_stop_position: None,
            server_id,
            conn_pool,
            conn_url,
        }
    }
}

impl BinlogIngestor<'_, '_, '_> {
    async fn connect(&self) -> Result<Conn, MySQLConnectorError> {
        Conn::new(self.conn_pool.clone())
            .await
            .map_err(|err| MySQLConnectorError::ConnectionFailure(self.conn_url.clone(), err))
    }

    async fn open_binlog(&mut self) -> Result<(), MySQLConnectorError> {
        let binlog_stream = self
            .connect()
            .await?
            .get_binlog_stream(
                mysql_async::BinlogRequest::new(self.server_id)
                    .with_filename(self.next_position.filename.as_slice())
                    .with_pos(self.next_position.position),
            )
            .await
            .map_err(MySQLConnectorError::BinlogOpenError)?;

        self.binlog_stream = Some(binlog_stream);

        self.local_stop_position = self.stop_position.as_ref().and_then(|stop_position| {
            if self.next_position.filename == stop_position.filename {
                Some(stop_position.position)
            } else {
                None
            }
        });

        Ok(())
    }

    pub async fn ingest(
        &mut self,
        tables: &mut [TableDefinition],
        schema_helper: SchemaHelper<'_>,
    ) -> Result<(), MySQLConnectorError> {
        if self.binlog_stream.is_none() {
            self.open_binlog().await?;
        }

        let mut table_cache = TableManager::new(tables);
        let mut schema_change_tracker = SchemaChangeTracker::new();

        'binlog_read: while let Some(result) = self.binlog_stream.as_mut().unwrap().next().await {
            match self.local_stop_position {
                Some(stop_position) if self.next_position.position >= stop_position => {
                    break 'binlog_read;
                }
                _ => {}
            }

            let binlog_event = match result {
                Ok(event) => event,
                Err(err) => {
                    if is_network_failure(&err) {
                        self.open_binlog().await?;
                        continue 'binlog_read;
                    } else {
                        Err(MySQLConnectorError::BinlogReadError(err))?
                    }
                }
            };

            let is_artificial = binlog_event
                .header()
                .flags()
                .contains(EventFlags::LOG_EVENT_ARTIFICIAL_F);

            if is_artificial {
                continue;
            }

            self.next_position.position = binlog_event.header().log_pos().into();

            let event_type = match binlog_event.header().event_type() {
                Ok(event_type) => event_type,
                _ => {
                    continue;
                }
            };

            use mysql_common::binlog::{consts::EventType::*, events::EventData::*};
            match event_type {
                ROTATE_EVENT => {
                    let rotate_event =
                        match binlog_event.read_data().map_err(binlog_io_error)?.unwrap() {
                            RotateEvent(rotate_event) => rotate_event,
                            _ => unreachable!(),
                        };

                    if rotate_event.name_raw() != self.next_position.filename.as_slice() {
                        self.next_position = BinlogPosition {
                            filename: rotate_event.name_raw().into(),
                            position: rotate_event.position(),
                        };

                        self.open_binlog().await?;
                    }

                    table_cache.handle_binlog_rotate();
                }

                QUERY_EVENT => {
                    let query_event =
                        match binlog_event.read_data().map_err(binlog_io_error)?.unwrap() {
                            QueryEvent(query_event) => query_event,
                            _ => unreachable!(),
                        };

                    let query = query_event.query_raw().trim_start();

                    if query == b"BEGIN"
                        && self
                            .ingestor
                            .handle_message(IngestionMessage::SnapshottingStarted)
                            .await
                            .is_err()
                    {
                        // If receiving side is closed, we can stop ingesting.
                        return Ok(());
                    } else if query.starts_with_case_insensitive(b"ALTER")
                        || query.starts_with_case_insensitive(b"DROP")
                    {
                        if schema_change_tracker.unknown_schema_change_occured {
                            // An unknown schema change has occured before, so granular checks might be inaccurate.
                            // The pending full schema check should suffice.
                        } else {
                            let query = String::from_utf8_lossy(query);
                            let dialect = &sqlparser::dialect::MySqlDialect {};
                            let result = sqlparser::parser::Parser::parse_sql(dialect, &query);
                            let schema = query_event.schema_raw();
                            match result {
                                Err(err) => {
                                    warn!("Failed to parse MySQL query {query:?}: {err}");
                                    // resort to manual schema verification
                                    schema_change_tracker.unknown_schema_change_occured();
                                }
                                Ok(statements) => {
                                    for statement in statements {
                                        use sqlparser::ast::{
                                            AlterTableOperation, ObjectType, Statement,
                                        };
                                        match statement {
                                            Statement::Drop {
                                                object_type: ObjectType::Schema,
                                                names,
                                                ..
                                            } => {
                                                for name in names {
                                                    let database_name =
                                                        object_name_to_string(&name);
                                                    if table_cache
                                                        .databases()
                                                        .contains(&database_name)
                                                    {
                                                        Err(MySQLConnectorError::BreakingSchemaChange(format!("Database \"{}\" was dropped", database_name)))?
                                                    }
                                                }
                                            }
                                            Statement::Drop {
                                                object_type: ObjectType::Table,
                                                names,
                                                ..
                                            } => {
                                                for name in names {
                                                    if let Some(table) = table_cache
                                                        .find_table_by_object_name(&name, schema)
                                                    {
                                                        Err(MySQLConnectorError::BreakingSchemaChange(format!("Table \"{}\" was dropped", table)))?
                                                    }
                                                }
                                            }
                                            Statement::AlterTable {
                                                name, operations, ..
                                            } => {
                                                if let Some(table) = table_cache
                                                    .find_table_by_object_name(&name, schema)
                                                {
                                                    let find_column =
                                                        |name: &sqlparser::ast::Ident| {
                                                            table.columns.iter().find(|cd| {
                                                                cd.name.eq_ignore_ascii_case(
                                                                    &name.value,
                                                                )
                                                            })
                                                        };
                                                    for operation in operations.iter() {
                                                        match operation {
                                                        AlterTableOperation::AddColumn {
                                                            ..
                                                        } => {
                                                            schema_change_tracker.column_order_changed_in(table.table_index);
                                                        }
                                                        AlterTableOperation::DropColumn {
                                                            column_name,
                                                            ..
                                                        } => {
                                                            if let Some(column) =
                                                                find_column(column_name)
                                                            {
                                                                Err(MySQLConnectorError::BreakingSchemaChange(format!("Column \"{}\" from table \"{}\" was dropped", column, table)))?
                                                            }
                                                            schema_change_tracker.column_order_changed_in(table.table_index);
                                                        }
                                                        AlterTableOperation::RenameColumn {
                                                            old_column_name,
                                                            new_column_name,
                                                        } => {
                                                            if !old_column_name
                                                                .value
                                                                .eq_ignore_ascii_case(
                                                                    &new_column_name.value,
                                                                )
                                                            {
                                                                if let Some(column) =
                                                                    find_column(old_column_name)
                                                                {
                                                                    Err(MySQLConnectorError::BreakingSchemaChange(format!("Column \"{}\" from table \"{}\" was renamed to \"{}\"", column, table, new_column_name.value)))?
                                                                }
                                                            }
                                                        }
                                                        AlterTableOperation::RenameTable {
                                                            table_name,
                                                        } => {
                                                            Err(MySQLConnectorError::BreakingSchemaChange(format!("Table \"{}\" was renamed to \"{}\"", table, object_name_to_string(table_name))))?
                                                        }
                                                        AlterTableOperation::ChangeColumn {
                                                            old_name,
                                                            new_name,
                                                            data_type,
                                                            options: _, // TODO: handle options changes
                                                        } => {
                                                            if let Some(column) = find_column(old_name) {
                                                                if !old_name.value.eq_ignore_ascii_case(&new_name.value) {
                                                                    Err(MySQLConnectorError::BreakingSchemaChange(format!("Column \"{}\" from table \"{}\" was renamed to \"{}\"", column, table, new_name.value)))?
                                                                }
                                                                let new_type = get_field_type_for_sql_type(data_type);
                                                                if new_type != column.typ {
                                                                    Err(MySQLConnectorError::BreakingSchemaChange(format!("Column \"{}\" from table \"{}\" changed data type from \"{}\" to \"{}\"", column, table, column.typ, new_type)))?
                                                                }
                                                            }
                                                        }
                                                        AlterTableOperation::AlterColumn {
                                                            column_name,
                                                            op,
                                                        } => {
                                                            if let Some(_column) = find_column(column_name) {
                                                                use sqlparser::ast::AlterColumnOperation;
                                                                match op {
                                                                    AlterColumnOperation::SetDefault { .. } |
                                                                    AlterColumnOperation::DropDefault => (), // TODO: handle options changes
                                                                    AlterColumnOperation::SetNotNull |
                                                                    AlterColumnOperation::DropNotNull |
                                                                    AlterColumnOperation::SetDataType {..} => unreachable!("MySQL does not support this statement {}", operation),
                                                                }
                                                            }
                                                        }
                                                        _ => (),
                                                    }
                                                    }
                                                }
                                            }
                                            _ => (),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                XID_EVENT => {
                    if self
                        .ingestor
                        .handle_message(IngestionMessage::SnapshottingDone)
                        .await
                        .is_err()
                    {
                        // If receiving side is closed, we can stop ingesting.
                        return Ok(());
                    }
                }

                WRITE_ROWS_EVENT | UPDATE_ROWS_EVENT | DELETE_ROWS_EVENT | WRITE_ROWS_EVENT_V1
                | UPDATE_ROWS_EVENT_V1 | DELETE_ROWS_EVENT_V1 => {
                    let event_data =
                        match binlog_event.read_data().map_err(binlog_io_error)?.unwrap() {
                            RowsEvent(event_data) => event_data,
                            _ => unreachable!(),
                        };

                    let rows_event = BinlogRowsEvent(event_data);

                    let binlog_table_id = rows_event.table_id();

                    let tme = self.get_tme(binlog_table_id)?;

                    if schema_change_tracker.unknown_schema_change_occured {
                        table_cache.refresh_full_schema(schema_helper).await?;
                        schema_change_tracker.clear();
                    }

                    if let Some(table_index) = table_cache.get_corresponding_table_index(tme) {
                        if schema_change_tracker
                            .column_order_changed
                            .contains(&table_index)
                        {
                            let tables = &schema_change_tracker.column_order_changed;
                            table_cache
                                .refresh_column_ordinals(schema_helper, tables)
                                .await?;
                            schema_change_tracker.clear();
                        }

                        let table = table_cache.get_table_details(table_index).unwrap();
                        self.handle_rows_event(&rows_event, &table, tme).await?;
                    }
                }

                event_type => {
                    trace!("other binlog event {event_type:?}");
                }
            }
        }

        Ok(())
    }

    async fn handle_rows_event<'a>(
        &self,
        rows_event: &BinlogRowsEvent<'_>,
        table: &TableDetails<'_>,
        tme: &TableMapEvent<'a>,
    ) -> Result<(), MySQLConnectorError> {
        for op in self.make_rows_operations(rows_event, table, tme) {
            if self
                .ingestor
                .handle_message(IngestionMessage::OperationEvent {
                    table_index: table.def.table_index,
                    op: op?,
                    id: None,
                })
                .await
                .is_err()
            {
                // If receiving side is closed, we can stop ingesting.
                return Ok(());
            }
        }

        Ok(())
    }

    fn get_tme(&self, binlog_table_id: u64) -> Result<&TableMapEvent<'_>, MySQLConnectorError> {
        self.binlog_stream
            .as_ref()
            .unwrap()
            .get_tme(binlog_table_id)
            .ok_or_else(|| {
                MySQLConnectorError::BinlogError(format!(
                    "Missing table-map-event for table_id: {binlog_table_id}"
                ))
            })
    }

    // Select the intersection between the columns present in the row and the columns in the table.
    //
    // Returns the index and field type of each selected column.
    //
    // # Parameters
    // - `row_columns`: The zero-based indexes of columns present in the binlog row.
    // - `table_definition`: The table definition.
    fn select_columns<'a>(
        &self,
        row_columns: Vec<usize>,
        table: &TableDetails<'a>,
    ) -> Vec<(usize, &'a FieldType)> {
        let columns = row_columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| table.columns.get(col).map(|cd| (i, &cd.typ)))
            .collect::<Vec<_>>();

        columns
    }

    fn rows_iter<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &TableDetails<'c>,
        tme: &'d TableMapEvent,
    ) -> impl Iterator<Item = Result<RowValues, MySQLConnectorError>> + 'r {
        let rows = rows_event.rows(tme);
        let selected_columns = (
            rows_event
                .columns_before_image()
                .map(|rows| self.select_columns(rows.collect(), table)),
            rows_event
                .columns_after_image()
                .map(|rows| self.select_columns(rows.collect(), table)),
        );

        rows.map(move |row| -> Result<RowValues, MySQLConnectorError> {
            fn into_fields(
                row: Option<BinlogRow>,
                selected_columns: Option<&Vec<(usize, &FieldType)>>,
            ) -> Result<Option<Vec<Field>>, MySQLConnectorError> {
                let value = if let Some(row) = row {
                    Some(row.into_fields(selected_columns.unwrap())?)
                } else {
                    None
                };
                Ok(value)
            }

            let row = row.map_err(binlog_io_error)?;

            let old_values = into_fields(row.0, selected_columns.0.as_ref())?;
            let new_values = into_fields(row.1, selected_columns.1.as_ref())?;

            Ok(RowValues {
                old_values,
                new_values,
            })
        })
    }

    fn make_rows_operations<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &TableDetails<'c>,
        tme: &'d TableMapEvent,
    ) -> impl Iterator<Item = Result<Operation, MySQLConnectorError>> + 'r {
        if rows_event.is_write() {
            OperationsIter::Iter1(self.make_insert_operations(rows_event, table, tme))
        } else if rows_event.is_update() {
            OperationsIter::Iter2(self.make_update_operations(rows_event, table, tme))
        } else if rows_event.is_delete() {
            OperationsIter::Iter3(self.make_delete_operations(rows_event, table, tme))
        } else {
            unreachable!()
        }
    }

    fn make_insert_operations<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &TableDetails<'c>,
        tme: &'d TableMapEvent,
    ) -> impl Iterator<Item = Result<Operation, MySQLConnectorError>> + 'r {
        self.rows_iter(rows_event, table, tme).map(|row| {
            let RowValues { new_values, .. } = row?;

            let op: Operation = Operation::Insert {
                new: Record::new(new_values.unwrap()),
            };

            Ok(op)
        })
    }

    fn make_update_operations<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &TableDetails<'c>,
        tme: &'d TableMapEvent,
    ) -> impl Iterator<Item = Result<Operation, MySQLConnectorError>> + 'r {
        self.rows_iter(rows_event, table, tme).map(|row| {
            let RowValues {
                old_values,
                new_values,
            } = row?;

            let op: Operation = Operation::Update {
                old: Record::new(old_values.unwrap()),
                new: Record::new(new_values.unwrap()),
            };

            Ok(op)
        })
    }

    fn make_delete_operations<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &TableDetails<'c>,
        tme: &'d TableMapEvent,
    ) -> impl Iterator<Item = Result<Operation, MySQLConnectorError>> + 'r {
        self.rows_iter(rows_event, table, tme).map(|row| {
            let RowValues { old_values, .. } = row?;

            let op: Operation = Operation::Delete {
                old: Record::new(old_values.unwrap()),
            };

            Ok(op)
        })
    }
}

struct SchemaChangeTracker {
    /// Table indexes for tables which had a column order change, usually caused by column addition or dropping.
    pub column_order_changed: HashSet<usize>,
    /// Some unkown schema change has occured. A a full schema refresh is needed and a rigourous check for breaking changes.
    pub unknown_schema_change_occured: bool,
}

impl SchemaChangeTracker {
    fn new() -> Self {
        Self {
            column_order_changed: HashSet::new(),
            unknown_schema_change_occured: false,
        }
    }

    fn column_order_changed_in(&mut self, table_index: usize) {
        self.column_order_changed.insert(table_index);
    }

    fn unknown_schema_change_occured(&mut self) {
        self.unknown_schema_change_occured = true;
    }

    fn clear(&mut self) {
        self.column_order_changed.clear();
        self.unknown_schema_change_occured = false;
    }
}

enum OperationsIter<I1, I2, I3> {
    Iter1(I1),
    Iter2(I2),
    Iter3(I3),
}

impl<I1, I2, I3> Iterator for OperationsIter<I1, I2, I3>
where
    I1: Iterator,
    I2: Iterator<Item = I1::Item>,
    I3: Iterator<Item = I2::Item>,
{
    type Item = I3::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OperationsIter::Iter1(iter) => iter.next(),
            OperationsIter::Iter2(iter) => iter.next(),
            OperationsIter::Iter3(iter) => iter.next(),
        }
    }
}

struct RowValues {
    pub old_values: Option<Vec<Field>>, // present in Update and Delete operations
    pub new_values: Option<Vec<Field>>, // present in Update and Insert operations
}

struct ColumnDefinitionsCache {
    cache: Vec<HashMap<usize, usize>>,
}

impl ColumnDefinitionsCache {
    pub fn new(table_definitions: &[TableDefinition]) -> Self {
        Self {
            cache: table_definitions
                .iter()
                .map(|td| {
                    td.columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| ((c.ordinal_position - 1) as usize, i))
                        .collect()
                })
                .collect(),
        }
    }

    pub fn get_columns_of_table<'a>(
        &self,
        td: &'a TableDefinition,
    ) -> Option<HashMap<usize, &'a ColumnDefinition>> {
        self.cache.get(td.table_index).map(|hashmap| {
            hashmap
                .iter()
                .map(|(&zero_based_ordinal, &i)| (zero_based_ordinal, &td.columns[i]))
                .collect()
        })
    }
}

pub fn binlog_io_error(err: std::io::Error) -> MySQLConnectorError {
    MySQLConnectorError::BinlogReadError(mysql_async::Error::Io(mysql_async::IoError::Io(err)))
}

impl<'a> IntoFields<'a> for BinlogRow {
    type Ctx = &'a [(usize, &'a FieldType)];

    fn into_fields(
        self,
        selected_columns: &[(usize, &FieldType)],
    ) -> Result<Vec<Field>, MySQLConnectorError> {
        let mut binlog_row = self;
        let mut fields = Vec::new();
        for (i, field_type) in selected_columns.iter().copied() {
            let value = binlog_row.take(i);
            fields.push(value.into_field(field_type)?);
        }
        Ok(fields)
    }
}

impl<'a, 'b> IntoField<'a> for Option<BinlogValue<'b>> {
    type Ctx = &'a FieldType;

    fn into_field(self, field_type: &FieldType) -> Result<Field, MySQLConnectorError> {
        let binlog_value = self;

        if binlog_value.is_none() {
            return Ok(Field::Null);
        }

        let field = match binlog_value.unwrap() {
            BinlogValue::Value(value) => value.into_field(field_type)?,
            BinlogValue::Jsonb(value) => Field::Json(value.into_json_value()?),
            BinlogValue::JsonDiff(_) => todo!(),
        };

        Ok(field)
    }
}

impl<'a> IntoJsonValue for binlog::jsonb::Value<'a> {
    fn into_json_value(self) -> Result<JsonValue, MySQLConnectorError> {
        use binlog::jsonb::Value::*;
        let json_value = match self {
            Null => JsonValue::NULL,
            Bool(v) => v.into(),
            I16(v) => v.into(),
            U16(v) => v.into(),
            I32(v) => v.into(),
            U32(v) => v.into(),
            I64(v) => v.into(),
            U64(v) => v.into(),
            F64(v) => v.into(),
            String(v) => (&*v.str()).into(),
            SmallArray(v) => v.into_json_value()?,
            LargeArray(v) => v.into_json_value()?,
            SmallObject(v) => v.into_json_value()?,
            LargeObject(v) => v.into_json_value()?,
            Opaque(v) => {
                let object: [(&str, JsonValue); 2] = [
                    ("value_type", (v.value_type() as u8).into()),
                    ("data", (&*v.data()).into()),
                ];
                object.into_iter().collect::<JsonObject>().into()
            }
        };

        Ok(json_value)
    }
}

impl<'a, T: StorageFormat> IntoJsonValue for ComplexValue<'a, T, Array> {
    fn into_json_value(self) -> Result<JsonValue, MySQLConnectorError> {
        Ok(self
            .iter()
            .map(|value| value.map_err(binlog_io_error)?.into_json_value())
            .collect::<Result<JsonArray, _>>()?
            .into())
    }
}

impl<'a, T: StorageFormat> IntoJsonValue for ComplexValue<'a, T, Object> {
    fn into_json_value(self) -> Result<JsonValue, MySQLConnectorError> {
        Ok(self
            .iter()
            .map(|entry| {
                let (key, value) = entry.map_err(binlog_io_error)?;
                Ok((key.value().into_owned(), value.into_json_value()?))
            })
            .collect::<Result<JsonObject, MySQLConnectorError>>()?
            .into())
    }
}

#[derive(Debug, Clone)]
pub struct TableDetails<'a> {
    pub def: &'a TableDefinition,
    pub columns: HashMap<usize, &'a ColumnDefinition>,
}

pub struct TableManager<'a> {
    tables: &'a mut [TableDefinition],
    binlog_table_id_to_table_index_map: HashMap<u64, usize>,
    known_missing_tme_table_ids: HashSet<u64>,
    column_definitions_cache: ColumnDefinitionsCache,
    databases: HashSet<String>,
}

impl TableManager<'_> {
    pub fn new(tables: &mut [TableDefinition]) -> TableManager<'_> {
        let column_definitions_cache = ColumnDefinitionsCache::new(tables);
        let databases = Self::get_unique_databases_set(tables);
        TableManager {
            tables,
            binlog_table_id_to_table_index_map: HashMap::new(),
            known_missing_tme_table_ids: HashSet::new(),
            column_definitions_cache,
            databases,
        }
    }

    pub fn handle_binlog_rotate(&mut self) {
        // binlog table ids are not guranteed to be consistent across binlog rotates
        self.binlog_table_id_to_table_index_map.clear();
        self.known_missing_tme_table_ids.clear();
    }

    /// Reload column ordinals after ALTER TABLE ADD COLUMN or DROP COLUMN.
    pub async fn refresh_column_ordinals(
        &mut self,
        schema_helper: SchemaHelper<'_>,
        table_indexes: &HashSet<usize>,
    ) -> Result<(), MySQLConnectorError> {
        let mut tables_to_refresh = self
            .tables
            .iter_mut()
            .filter(|td| table_indexes.contains(&td.table_index))
            .collect::<Vec<_>>();

        schema_helper
            .refresh_column_ordinals(tables_to_refresh.as_mut_slice())
            .await?;

        self.column_definitions_cache = ColumnDefinitionsCache::new(self.tables);

        Ok(())
    }

    // Refresh the entire schema after an ALTER TABLE.
    // This is a last resort when granular schema changes are not known.
    pub async fn refresh_full_schema(
        &mut self,
        schema_helper: SchemaHelper<'_>,
    ) -> Result<(), MySQLConnectorError> {
        schema_helper
            .refresh_schema_and_check_for_breaking_changes(self.tables)
            .await?;

        self.column_definitions_cache = ColumnDefinitionsCache::new(self.tables);

        Ok(())
    }

    pub fn get_table_details(&self, table_index: usize) -> Option<TableDetails> {
        self.tables.get(table_index).map(|td| TableDetails {
            def: td,
            columns: self
                .column_definitions_cache
                .get_columns_of_table(td)
                .unwrap(),
        })
    }

    pub fn get_corresponding_table_index(&mut self, tme: &TableMapEvent<'_>) -> Option<usize> {
        let binlog_table_id = tme.table_id();
        if let Some(&found) = self
            .binlog_table_id_to_table_index_map
            .get(&binlog_table_id)
        {
            Some(found)
        } else if self.known_missing_tme_table_ids.contains(&binlog_table_id) {
            None
        } else {
            // see if we can find it
            if let Some(&TableDefinition { table_index, .. }) = self.tables.iter().find(
                |TableDefinition {
                     table_name,
                     database_name,
                     ..
                 }| {
                    database_name.as_bytes() == tme.database_name_raw()
                        && table_name.as_bytes() == tme.table_name_raw()
                },
            ) {
                self.binlog_table_id_to_table_index_map
                    .insert(binlog_table_id, table_index);
                Some(table_index)
            } else {
                self.known_missing_tme_table_ids.insert(binlog_table_id);
                None
            }
        }
    }

    pub fn find_table_by_object_name(
        &self,
        object_name: &sqlparser::ast::ObjectName,
        fallback_schema: &[u8],
    ) -> Option<&TableDefinition> {
        let object_name = &object_name.0;
        if object_name.is_empty() || object_name.len() > 2 {
            return None;
        }
        let table_name = &object_name.last().unwrap().value;
        let database_name = if object_name.len() > 1 {
            object_name.first().unwrap().value.as_str().into()
        } else {
            String::from_utf8_lossy(fallback_schema)
        };
        let database_name = database_name.deref();
        self.tables.iter().find(|td| {
            td.table_name.eq_ignore_ascii_case(table_name)
                && td.database_name.eq_ignore_ascii_case(database_name)
        })
    }

    pub fn databases(&self) -> &HashSet<String> {
        &self.databases
    }

    fn get_unique_databases_set(tables: &[TableDefinition]) -> HashSet<String> {
        tables.iter().map(|td| td.database_name.clone()).collect()
    }
}

struct BinlogRowsEvent<'a>(binlog::events::RowsEventData<'a>);

macro_rules! rows_event_apply {
    (
        $event_data:expr,
        event.$($op:tt)*
    ) => {
        {
            use mysql_common::binlog::events::RowsEventData::*;
            match $event_data {
                WriteRowsEvent(event) => event.$($op)*,
                UpdateRowsEvent(event) => event.$($op)*,
                DeleteRowsEvent(event) => event.$($op)*,
                WriteRowsEventV1(event) => event.$($op)*,
                UpdateRowsEventV1(event) => event.$($op)*,
                DeleteRowsEventV1(event) => event.$($op)*,
                _ => unreachable!(),
            }
        }
    };
}

impl<'a> BinlogRowsEvent<'a> {
    pub fn table_id(&self) -> u64 {
        rows_event_apply!(&self.0, event.table_id())
    }

    pub fn rows(&'a self, tme: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        rows_event_apply!(&self.0, event.rows(tme))
    }

    pub fn columns_before_image(&'a self) -> Option<impl Iterator<Item = usize> + 'a> {
        use binlog::events::RowsEventData::*;
        let value = match &self.0 {
            UpdateRowsEventV1(event) => Some(event.columns_before_image()),
            DeleteRowsEventV1(event) => Some(event.columns_before_image()),
            UpdateRowsEvent(event) => Some(event.columns_before_image()),
            DeleteRowsEvent(event) => Some(event.columns_before_image()),
            _ => None,
        };
        value.map(|bitslice| bitslice.iter_ones())
    }

    pub fn columns_after_image(&'a self) -> Option<impl Iterator<Item = usize> + 'a> {
        use binlog::events::RowsEventData::*;
        let value = match &self.0 {
            UpdateRowsEventV1(event) => Some(event.columns_after_image()),
            WriteRowsEventV1(event) => Some(event.columns_after_image()),
            UpdateRowsEvent(event) => Some(event.columns_after_image()),
            WriteRowsEvent(event) => Some(event.columns_after_image()),
            _ => None,
        };
        value.map(|bitslice| bitslice.iter_ones())
    }

    pub fn is_write(&self) -> bool {
        use binlog::events::RowsEventData::*;
        matches!(&self.0, WriteRowsEventV1(_) | WriteRowsEvent(_))
    }

    pub fn is_update(&self) -> bool {
        use binlog::events::RowsEventData::*;
        matches!(&self.0, UpdateRowsEventV1(_) | UpdateRowsEvent(_))
    }

    pub fn is_delete(&self) -> bool {
        use binlog::events::RowsEventData::*;
        matches!(&self.0, DeleteRowsEventV1(_) | DeleteRowsEvent(_))
    }
}

#[cfg(test)]
mod tests {

    use dozer_ingestion_connector::dozer_types::{
        json_types::json,
        types::{Field, FieldType},
    };

    use mysql_common::{
        binlog::{
            jsonb::{self, JsonbString, JsonbType, OpaqueValue},
            value::BinlogValue,
        },
        constants::ColumnType,
        io::ParseBuf,
        Value,
    };

    use crate::conversion::IntoField;

    #[test]
    fn test_field_conversion() {
        use jsonb::Value::*;

        assert_eq!(
            Field::UInt(0),
            Some(BinlogValue::Value(Value::UInt(0)))
                .into_field(&FieldType::UInt)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(null)),
            Some(BinlogValue::Jsonb(Null))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(2.0)),
            Some(BinlogValue::Jsonb(I16(2)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(3.0)),
            Some(BinlogValue::Jsonb(I32(3)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(4.0)),
            Some(BinlogValue::Jsonb(U16(4)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(5.0)),
            Some(BinlogValue::Jsonb(U32(5)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(6.0)),
            Some(BinlogValue::Jsonb(I64(6)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(7.0)),
            Some(BinlogValue::Jsonb(U64(7)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!(8.0)),
            Some(BinlogValue::Jsonb(F64(8.0)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!("9")),
            Some(BinlogValue::Jsonb(String(JsonbString::new(vec![b'9']))))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(json!([10.0])),
            Some(BinlogValue::Jsonb(SmallArray(
                ParseBuf(&[1, 0, 7, 0, JsonbType::JSONB_TYPE_INT16 as u8, 10, 0])
                    .parse(())
                    .unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(json!([])),
            Some(BinlogValue::Jsonb(LargeArray(
                ParseBuf(&[0, 0, 0, 0, 8, 0, 0, 0]).parse(()).unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(json!({"k": 12.0})),
            Some(BinlogValue::Jsonb(SmallObject(
                ParseBuf(&[
                    1,
                    0,
                    12,
                    0,
                    11,
                    0,
                    1,
                    0,
                    JsonbType::JSONB_TYPE_INT16 as u8,
                    12,
                    0,
                    b'k',
                ])
                .parse(())
                .unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(json!({})),
            Some(BinlogValue::Jsonb(LargeObject(
                ParseBuf(&[0, 0, 0, 0, 8, 0, 0, 0]).parse(()).unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(json!({"value_type": ColumnType::MYSQL_TYPE_TINY as u8, "data": "a"})),
            Some(BinlogValue::Jsonb(Opaque(OpaqueValue::new(
                ColumnType::MYSQL_TYPE_TINY,
                vec![b'a']
            ))))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(json!(true)),
            Some(BinlogValue::Jsonb(Bool(true)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Null,
            None::<BinlogValue<'_>>.into_field(&FieldType::Int).unwrap(),
        );
    }
}

trait ByteSliceExt {
    fn trim_start(&self) -> &[u8];
    fn starts_with_case_insensitive(&self, prefix: &[u8]) -> bool;
}

impl ByteSliceExt for [u8] {
    fn trim_start(&self) -> &[u8] {
        for i in 0..self.len() {
            if !self[i].is_ascii_whitespace() {
                return &self[i..];
            }
        }
        &[]
    }

    fn starts_with_case_insensitive(&self, prefix: &[u8]) -> bool {
        if self.len() < prefix.len() {
            false
        } else {
            self[..prefix.len()].eq_ignore_ascii_case(prefix)
        }
    }
}

fn object_name_to_string(object_name: &sqlparser::ast::ObjectName) -> String {
    object_name
        .0
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}
