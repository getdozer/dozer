use super::{
    conversion::{IntoField, IntoFields, IntoJsonValue},
    schema::{ColumnDefinition, TableDefinition},
};
use crate::{
    errors::{ConnectorError, MySQLConnectorError},
    ingestion::Ingestor,
};
use dozer_types::{
    ingestion_types::IngestionMessage,
    log::trace,
    types::{FieldType, Operation, Record},
};
use dozer_types::{json_types::JsonValue, types::Field};
use futures::StreamExt;
use mysql_async::{prelude::Queryable, BinlogStream, Conn, Pool};
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
use std::collections::{BTreeMap, HashMap, HashSet};

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

pub struct BinlogIngestor<'a, 'b, 'c, 'd, 'e> {
    ingestor: &'a Ingestor,
    binlog_stream: Option<BinlogStream>,
    tables: &'b [TableDefinition],
    next_position: BinlogPosition,
    stop_position: Option<BinlogPosition>,
    local_stop_position: Option<u64>,
    server_id: u32,
    txn: &'c mut u64,
    conn_pool: &'d Pool,
    conn_url: &'e String,
    column_definitions_cache: ColumnDefinitionsCache<'b>,
}

impl<'a, 'b, 'c, 'd, 'e> BinlogIngestor<'a, 'b, 'c, 'd, 'e> {
    pub fn new(
        ingestor: &'a Ingestor,
        tables: &'b [TableDefinition],
        start_position: BinlogPosition,
        stop_position: Option<BinlogPosition>,
        server_id: u32,
        txn: &'c mut u64,
        (conn_pool, conn_url): (&'d Pool, &'e String),
    ) -> Self {
        Self {
            ingestor,
            binlog_stream: None,
            tables,
            next_position: start_position,
            stop_position,
            local_stop_position: None,
            server_id,
            txn,
            conn_pool,
            conn_url,
            column_definitions_cache: ColumnDefinitionsCache::new(tables),
        }
    }
}

impl BinlogIngestor<'_, '_, '_, '_, '_> {
    async fn connect(&self) -> Result<Conn, MySQLConnectorError> {
        self.conn_pool
            .get_conn()
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

    pub async fn ingest(&mut self) -> Result<(), ConnectorError> {
        if self.binlog_stream.is_none() {
            self.open_binlog().await?;
        }

        let mut seq_no = 0;
        let mut table_cache = BinlogTableCache::new(self.tables);

        'binlog_read: while let Some(event) = self.binlog_stream.as_mut().unwrap().next().await {
            let binlog_event = event.map_err(MySQLConnectorError::BinlogReadError)?;

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

                    table_cache.binlog_rotate();
                }

                QUERY_EVENT => {
                    let query_event =
                        match binlog_event.read_data().map_err(binlog_io_error)?.unwrap() {
                            QueryEvent(query_event) => query_event,
                            _ => unreachable!(),
                        };

                    if query_event.query_raw() == b"BEGIN" {
                        self.ingestor
                            .handle_message(IngestionMessage::new_snapshotting_started(
                                *self.txn, seq_no,
                            ))
                            .map_err(ConnectorError::IngestorError)?;
                        seq_no += 1;
                    }
                }

                XID_EVENT => {
                    self.ingestor
                        .handle_message(IngestionMessage::new_snapshotting_done(*self.txn, seq_no))
                        .map_err(ConnectorError::IngestorError)?;

                    *self.txn += 1;
                    seq_no = 0;
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

                    if let Some(table) = table_cache.get_corresponding_table(tme) {
                        self.handle_rows_event(&rows_event, table, tme, &mut seq_no)?;
                    }
                }

                event_type => {
                    trace!("other binlog event {event_type:?}");
                }
            }

            match self.local_stop_position {
                Some(stop_position) if self.next_position.position >= stop_position => {
                    break 'binlog_read;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn handle_rows_event(
        &self,
        rows_event: &BinlogRowsEvent<'_>,
        table: &TableDefinition,
        tme: &TableMapEvent,
        seq_no: &mut u64,
    ) -> Result<(), ConnectorError> {
        for op in self.make_rows_operations(rows_event, table, tme) {
            self.ingestor
                .handle_message(IngestionMessage::new_op(
                    *self.txn,
                    *seq_no,
                    table.table_index,
                    op?,
                ))
                .map_err(ConnectorError::IngestorError)?;
        }
        *seq_no += 1;

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
    fn select_columns(
        &self,
        row_columns: Vec<usize>,
        table_definition: &TableDefinition,
    ) -> Vec<(usize, &FieldType)> {
        let table_columns = self
            .column_definitions_cache
            .get_columns_of_table(table_definition.table_index)
            .unwrap();
        let columns = row_columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| table_columns.get(col).map(|cd| (i, &cd.typ)))
            .collect::<Vec<_>>();

        columns
    }

    fn rows_iter<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &'c TableDefinition,
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
        table: &'c TableDefinition,
        tme: &'d TableMapEvent,
    ) -> Box<dyn Iterator<Item = Result<Operation, MySQLConnectorError>> + 'r> {
        if rows_event.is_write() {
            Box::new(self.make_insert_operations(rows_event, table, tme))
        } else if rows_event.is_update() {
            Box::new(self.make_update_operations(rows_event, table, tme))
        } else if rows_event.is_delete() {
            Box::new(self.make_delete_operations(rows_event, table, tme))
        } else {
            unreachable!()
        }
    }

    fn make_insert_operations<'a: 'r, 'b: 'r, 'c: 'r, 'd: 'r, 'r>(
        &'a self,
        rows_event: &'b BinlogRowsEvent<'_>,
        table: &'c TableDefinition,
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
        table: &'c TableDefinition,
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
        table: &'c TableDefinition,
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

struct RowValues {
    pub old_values: Option<Vec<Field>>, // present in Update and Delete operations
    pub new_values: Option<Vec<Field>>, // present in Update and Insert operations
}

struct ColumnDefinitionsCache<'a> {
    cache: Vec<HashMap<usize, &'a ColumnDefinition>>,
}

impl<'a> ColumnDefinitionsCache<'a> {
    pub fn new(table_definitions: &'a [TableDefinition]) -> Self {
        Self {
            cache: table_definitions
                .iter()
                .map(|td| {
                    td.columns
                        .iter()
                        .map(|c| ((c.ordinal_position - 1) as usize, c))
                        .collect()
                })
                .collect(),
        }
    }

    pub fn get_columns_of_table(
        &self,
        table_index: usize,
    ) -> Option<&HashMap<usize, &ColumnDefinition>> {
        self.cache.get(table_index)
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
            Null => JsonValue::Null,
            Bool(v) => JsonValue::Bool(v),
            I16(v) => JsonValue::Number((v as f64).into()),
            U16(v) => JsonValue::Number((v as f64).into()),
            I32(v) => JsonValue::Number((v as f64).into()),
            U32(v) => JsonValue::Number((v as f64).into()),
            I64(v) => JsonValue::Number((v as f64).into()),
            U64(v) => JsonValue::Number((v as f64).into()),
            F64(v) => JsonValue::Number(v.into()),
            String(v) => JsonValue::String(v.str().into_owned()),
            SmallArray(v) => v.into_json_value()?,
            LargeArray(v) => v.into_json_value()?,
            SmallObject(v) => v.into_json_value()?,
            LargeObject(v) => v.into_json_value()?,
            Opaque(v) => JsonValue::Object({
                let mut map = BTreeMap::new();
                map.insert(
                    "value_type".into(),
                    JsonValue::from(u8::from(v.value_type()) as usize),
                );
                map.insert("data".into(), JsonValue::from(v.data()));
                map
            }),
        };

        Ok(json_value)
    }
}

impl<'a, T: StorageFormat> IntoJsonValue for ComplexValue<'a, T, Array> {
    fn into_json_value(self) -> Result<JsonValue, MySQLConnectorError> {
        Ok(JsonValue::Array({
            let mut vec = Vec::new();
            for value in self.iter() {
                vec.push(value.map_err(binlog_io_error)?.into_json_value()?);
            }
            vec
        }))
    }
}

impl<'a, T: StorageFormat> IntoJsonValue for ComplexValue<'a, T, Object> {
    fn into_json_value(self) -> Result<JsonValue, MySQLConnectorError> {
        Ok(JsonValue::Object({
            let mut map = BTreeMap::new();
            for entry in self.iter() {
                let (key, value) = entry.map_err(binlog_io_error)?;
                map.insert(key.value().into_owned(), value.into_json_value()?);
            }
            map
        }))
    }
}

pub struct BinlogTableCache<'a> {
    tables: &'a [TableDefinition],
    binlog_table_id_to_table_map: HashMap<u64, &'a TableDefinition>,
    known_missing: HashSet<u64>,
}

impl BinlogTableCache<'_> {
    pub fn new(tables: &[TableDefinition]) -> BinlogTableCache<'_> {
        BinlogTableCache {
            tables,
            binlog_table_id_to_table_map: HashMap::new(),
            known_missing: HashSet::new(),
        }
    }

    pub fn binlog_rotate(&mut self) {
        // binlog table ids are not guranteed to be consistent across binlog rotates
        self.binlog_table_id_to_table_map.clear();
        self.known_missing.clear();
    }

    pub fn get_corresponding_table(&mut self, tme: &TableMapEvent<'_>) -> Option<&TableDefinition> {
        let binlog_table_id = tme.table_id();
        if let Some(found) = self.binlog_table_id_to_table_map.get(&binlog_table_id) {
            Some(found)
        } else if self.known_missing.contains(&binlog_table_id) {
            None
        } else {
            // see if we can find it
            if let Some(value) = self.tables.iter().find(
                |TableDefinition {
                     table_name,
                     database_name,
                     ..
                 }| {
                    database_name.as_bytes() == tme.database_name_raw()
                        && table_name.as_bytes() == tme.table_name_raw()
                },
            ) {
                self.binlog_table_id_to_table_map
                    .insert(binlog_table_id, value);
                Some(value)
            } else {
                self.known_missing.insert(binlog_table_id);
                None
            }
        }
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
    use std::collections::BTreeMap;

    use dozer_types::{
        json_types::JsonValue,
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

    use crate::connectors::mysql::conversion::IntoField;

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
            Field::Json(JsonValue::Null),
            Some(BinlogValue::Jsonb(Null))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(2.0.into())),
            Some(BinlogValue::Jsonb(I16(2)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(3.0.into())),
            Some(BinlogValue::Jsonb(I32(3)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(4.0.into())),
            Some(BinlogValue::Jsonb(U16(4)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(5.0.into())),
            Some(BinlogValue::Jsonb(U32(5)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(6.0.into())),
            Some(BinlogValue::Jsonb(I64(6)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(7.0.into())),
            Some(BinlogValue::Jsonb(U64(7)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Number(8.0.into())),
            Some(BinlogValue::Jsonb(F64(8.0)))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::String("9".into())),
            Some(BinlogValue::Jsonb(String(JsonbString::new(vec![b'9']))))
                .into_field(&FieldType::Json)
                .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Array(vec![JsonValue::Number(10.0.into())])),
            Some(BinlogValue::Jsonb(SmallArray(
                ParseBuf(&[1, 0, 7, 0, JsonbType::JSONB_TYPE_INT16 as u8, 10, 0])
                    .parse(())
                    .unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Array(vec![])),
            Some(BinlogValue::Jsonb(LargeArray(
                ParseBuf(&[0, 0, 0, 0, 8, 0, 0, 0]).parse(()).unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Object({
                let mut map = BTreeMap::new();
                map.insert("k".into(), JsonValue::Number(12.0.into()));
                map
            })),
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
            Field::Json(JsonValue::Object(BTreeMap::new())),
            Some(BinlogValue::Jsonb(LargeObject(
                ParseBuf(&[0, 0, 0, 0, 8, 0, 0, 0]).parse(()).unwrap(),
            )))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Object({
                let mut map = BTreeMap::new();
                map.insert(
                    "value_type".into(),
                    JsonValue::from(u8::from(ColumnType::MYSQL_TYPE_TINY) as usize),
                );
                map.insert("data".into(), JsonValue::String('a'.into()));
                map
            })),
            Some(BinlogValue::Jsonb(Opaque(OpaqueValue::new(
                ColumnType::MYSQL_TYPE_TINY,
                vec![b'a']
            ))))
            .into_field(&FieldType::Json)
            .unwrap()
        );

        assert_eq!(
            Field::Json(JsonValue::Bool(true)),
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
