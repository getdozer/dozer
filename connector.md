## Trait

Every connector to external database needs to implement connector trait (/dozer-ingestion/src/connectors/mod.rs)

| ```rust fn validate(&self, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError>```                                                                         |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| This function is supposed to validate connector configuration and connection to database. It also validates tables, columns existence and user details/permissions. |

| ```rust fn validate_schemas(&self, tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError>;```                                                                     |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| This function purpose is to validate schemas, which are used as sources for data ingestion. It should return error when column type is not supported by existing dozer types. |

| ```rust fn get_schemas(&self,table_names: Option<Vec<TableInfo>>,) -> Result<Vec<SchemaWithChangesType>, ConnectorError>;```                                                                              |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| This function is used for getting mapped external database schema to dozer schema. Also, in result of schema definition, developer also should provide `ReplicationChangesTrackingType` (described below) |

| ```rust fn initialize(&mut self,ingestor: Arc<RwLock<Ingestor>>,tables: Option<Vec<TableInfo>>,) -> Result<(), ConnectorError>;``` |
|------------------------------------------------------------------------------------------------------------------------------------|
| In this method, developer passes ingestor and tables information, which will later be used during ingestion process.               |

|                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ```rust fn start(&self, from_seq: Option<(u64, u64)>) -> Result<(), ConnectorError>;```                                                                                                                                                                                                                                                                |                                                                                             |
 | This function is responsible for all ingestion process. It has single parameter, which is used to resume ingestion if was stopped. That tuple contains two values - `(u64, u64)`, first value is lsn of transaction and second value is seq no of last consumer record in transaction. It used to allow connector continue from middle of transaction. |

#### Replication changes tracking types

| Type        |                                                                                          |
|-------------|------------------------------------------------------------------------------------------|
| FullChanges | Connector gets old record on delete/update operations                                    | 
| OnlyPK      | Connector only gets PK of old record on delete/update operations                         |
| Nothing     | Connector cannot get any info about old record. In other words, the table is append only |

## Tables and columns selection
Selection of which tables and columns will be used in ingestion is defined in `sources` configuration.
Structure of that configuration part is this
```yaml
    name: users
    connection: !Ref pg_data_connection
    table_name: userdata
    columns:
      - gender
      - email
```

From this configuration, `table_name` is used as table name in external database and `name` is used in dozer transformations.
Other property `connection` is reference to connection, which are already defined in `connections` configuration.
`columns` property is used to restrict list of used columns from external database. If this value is empty array, ingestion will fetch all columns of that table.

## Schemas
Every external schema should be mapped using dozer types. Latest types definitions can be found in https://getdozer.io/docs/reference/data_types . If type is not supported, connector should return error, during schema validation step.
During ingestion data should be cast to same type as it was defined in schema.

## Replay support 
During pipeline start, start function receives tuple `from_seq: (u64, u64)`. That tuple is used to tell last message lsn and seq no. One lsn is shared for all operations inside single transaction, while second parameter is used for determing how many messages were successfully processed from transaction.

## Unit tests
Unit tests are only possible in places where connection to external database is not required. It is important to have unit tests for schema mapping and data casting to dozer types. More complex tests should be implemented using E2E tests.

## E2E tests
More complex tests requires to have connection real database. Such tests cases expects have several things:
- Database infrastructure (preferably created in docker container(s))
- Connection configuration (with placeholders)
- Test case should be possible to run without doing any manual modifications in database.

