# `Connector` specification

## Methods

### `types_mapping`

Returns all the external types and their corresponding Dozer types.

Returns:

- `Vec<(String, Option<FieldType>)>` - Vector of tuples of external type name and corresponding Dozer type. If the external type is not supported, `None` should be returned.

### `validate_connection`

Validates the connector's connection level properties.

Returns:

- `Result<(), ConnectorError>` - `Ok` if the connection can be made and can be used for ingestion, `Err` otherwise.

### `list_tables`

Lists all the table names in the connector.

Returns:

- `Result<Vec<TableIdentifier>, ConnectorError>` - Vector of table identifiers. See [TableIdentifier](#tableidentifier) for more details.

### `validate_tables`

Validates the connector's table level properties for each table.

Arguments:

- `tables: &[TableIdentifier]` - Vector of tables to validate.

Returns:

- `Result<(), ConnectorError>` - `Ok` if all the tables can be used for ingestion, `Err` otherwise.

This validation does not validate if columns can be mapped to Dozer types. It only validates table level properties like if the table exists.

### `list_columns`

Lists all the column names for each table.

Arguments:

- `tables: Vec<TableIdentifier>` - Vector of tables to list columns for.

Returns:

- `Result<Vec<TableInfo>, ConnectorError>` - Vector of table info. See [TableInfo](#tableinfo) for more details.

If the method succeeds, the returned vector must have the same number of elements as the `tables` argument, and the order must be the same.

### `get_schemas`

Gets the schema for each table. Only requested columns need to be mapped.

Arguments:

- `table_infos: &[TableInfo]` - Vector of table info. See [TableInfo](#tableinfo) for more details.

Returns:

- `Result<Vec<SourceSchemaResult>, ConnectorError>` - Vector of source schema results. See [SourceSchemaResult](#sourceschemaresult) for more details.

The outer level error should be returned upon connection failure. If connection is successful, the inner level error should be returned for each table.

The returned `Vec<SourceSchemaResult>` must have the same number of elements as the `table_infos` argument, and the order must be the same.

### `start`

Starts outputting data from `tables` to `ingestor`.

Arguments:

- `ingestor: &Ingestor` - Ingestor to output data to.
- `tables: Vec<TableInfo>` - Vector of table info. See [TableInfo](#tableinfo) for more details.

Returns:

- `Result<(), ConnectorError>` - `Ok` if the all data is successfully output, `Err` otherwise.

See [Ingestor](#ingestor) for contract of the data that Dozer expects.

## Ingestor

`Ingestor` is the sender side of a spsc channel. The message is defined in `IngestionMessage`.

### `IngestionMessage`

`identifier` must be monotonically increasing for each connector, ordering is defined by `Rust`'s `PartialOrd`.

```rust
/// Messages that connectors send to Dozer.
pub struct IngestionMessage {
    /// The message's identifier, must be unique in a connector.
    pub identifier: OpIdentifier,
    /// The message kind.
    pub kind: IngestionMessageKind,
}
```

### `OpIdentifier`

```rust
/// A identifier made of two `u64`s.
pub struct OpIdentifier {
    /// High 64 bits of the identifier.
    pub txid: u64,
    /// Low 64 bits of the identifier.
    pub seq_in_tx: u64,
}
```

### `IngestionMessageKind`

`SnapshottingDone` should be sent from connectors that have distinct snapshot phase and streaming phase, such as postgres.

For connectors that only streams, such as kafka, `SnapshottingDone` should not be sent.

```rust
/// All possible kinds of `IngestionMessage`.
pub enum IngestionMessageKind {
    /// A CDC event.
    OperationEvent(Operation),
    /// A connector uses this message kind to notify Dozer that a initial snapshot of the source table is done,
    /// and the data is up-to-date until next CDC event.
    SnapshottingDone,
}
```

### `Operation`

```rust
/// A CDC event.
pub enum Operation {
    Delete { old: Record },
    Insert { new: Record },
    Update { old: Record, new: Record },
}
```

### `Record`

`schema_id` is how Dozer knows which table this record belongs to. It must be `Some` and the same as the returned `SchemaIdentifier` from `get_schemas` for the table it originates from.

`values` must be of the same length, order, and type as the `fields` in corresponding `Schema`, expect for `old` in `Delete` and `Update` operation, see [Omitting fields](#omitting-fields-in-delete-and-update-operations).

```rust
pub struct Record {
    /// Schema implemented by this Record
    pub schema_id: Option<SchemaIdentifier>,
    /// List of values, following the definitions of `fields` of the associated schema
    pub values: Vec<Field>,
}
```

### Omitting fields in `Delete` and `Update` operations

If a connector declares a table's `CdcType` to be `OnlyPK`, the connector can omit the fields which is not part of the primary key of the `old` record of `Delete` and `Update` operations.

However, the `values` must still be of the same length and order as the `fields` in corresponding `Schema`, omitted fields can be filled with `Field::Null`.

## Data Structures

### `TableIdentifier`

```rust
/// Unique identifier of a source table. A source table must have a `name`, optionally under a `schema` scope.
pub struct TableIdentifier {
    /// The `schema` scope of the table.
    ///
    /// Connector that supports schema scope must decide on a default schema, that doesn't must assert that `schema.is_none()`.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
}
```

### `TableInfo`

```rust
/// `TableIdentifier` with column names.
pub struct TableInfo {
    /// The `schema` scope of the table.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
    /// The column names to be mapped.
    pub column_names: Vec<String>,
}
```

### `SourceSchemaResult`

```rust
/// Result of mapping one source table schema to Dozer schema.
pub type SourceSchemaResult = Result<SourceSchema, ConnectorError>;
```

### `SourceSchema`

```rust
/// A source table's schema and CDC type.
pub struct SourceSchema {
    /// Dozer schema mapped from the source table. Columns are already filtered based on `TableInfo.column_names`.
    pub schema: Schema,
    /// The source table's CDC type.
    pub cdc_type: CdcType,
}
```

### `Schema`

TODO.

### `CdcType`

```rust
/// A source table's CDC event type.
pub enum CdcType {
    /// Connector gets old record on delete/update operations.
    FullChanges,
    /// Connector only gets PK of old record on delete/update operations.
    OnlyPK,
    /// Connector cannot get any info about old records. In other words, the table is append-only.
    Nothing,
}
```
