use std::collections::HashMap;

use bson::{doc, Bson, Document, Timestamp};
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        self,
        errors::{internal::BoxedError, types::DeserializationError},
        json_types::{serde_json_to_json_value, JsonValue},
        models::ingestion_types::IngestionMessage,
        node::OpIdentifier,
        thiserror::{self, Error},
        types::{Field, FieldDefinition, FieldType, Operation, Record, SourceDefinition},
    },
    futures::{stream::FuturesUnordered, StreamExt, TryFutureExt, TryStreamExt},
    tokio::{
        self,
        sync::mpsc::{channel, Sender},
    },
    CdcType, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};
use mongodb::{
    change_stream::event::ChangeStreamEvent,
    error::{CommandError, ErrorKind},
    options::{ChangeStreamOptions, ClientOptions, ConnectionString},
};

pub use bson;
pub use mongodb;

#[derive(Error, Debug)]
pub enum MongodbConnectorError {
    #[error("Failed to parse connection string. {0}")]
    ParseConnectionString(#[source] mongodb::error::Error),

    #[error("Server is not part of a replica set")]
    NotAReplicaSet,

    #[error("Server is sharded, which is currently not supported")]
    Sharded,

    #[error("Failed to connect to mongodb with the specified configuration. {0}")]
    ConnectionFailure(#[source] mongodb::error::Error),

    #[error("Failed to list databases. {0}")]
    ListTablesError(#[source] mongodb::error::Error),

    #[error("Failed to read collection snapshot. {0}")]
    SnapshotReadError(#[source] mongodb::error::Error),

    #[error("Failed to start a change stream for collection. {0}")]
    ReplicationError(#[source] mongodb::error::Error),

    #[error("Failed to parse change stream data for collection. {0}")]
    ReplicationDataError(#[source] DeserializationError),

    #[error("Change stream was invalidated because the replicated collection was renamed or dropped while replicating")]
    ReplicationStreamInvalidated,

    #[error("No database specified in connection string")]
    NoDatabaseError,

    #[error("Capped collections cannot be used as sources. Collection: {0}")]
    CappedCollection(String),

    #[error("Collection should have pre- and post-images enabled. Collection: {0}")]
    NoPrePostImages(String),

    #[error("Missing permissions: {}", .0.iter().map(|(table, permissions)| format!("{table}: [{}]", permissions.join(", "))).collect::<Vec<_>>().join(", "))]
    MissingPermissions(Vec<(String, Vec<String>)>),
}

use MongodbConnectorError::*;

#[derive(Debug)]
pub struct MongodbConnector {
    conn_string: String,
}

#[derive(Default, Clone, Copy)]
struct Privs {
    find: bool,
    watch: bool,
}

impl std::ops::BitOrAssign for Privs {
    fn bitor_assign(&mut self, rhs: Self) {
        self.find |= rhs.find;
        self.watch |= rhs.watch;
    }
}

impl std::ops::BitOr for Privs {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Privs {
            find: self.find || rhs.find,
            watch: self.watch || rhs.watch,
        }
    }
}

async fn start_session(
    client: &mongodb::Client,
) -> Result<mongodb::ClientSession, MongodbConnectorError> {
    let session_options = mongodb::options::SessionOptions::builder()
        .snapshot(true)
        .build();

    // Check if we can start a session with our read and write concerns. This
    // will validate that the connected server is part of a replica set
    client
        .start_session(Some(session_options))
        .await
        .map_err(|e| match *e.kind {
            ErrorKind::Command(CommandError { code: 123, .. }) => {
                MongodbConnectorError::NotAReplicaSet
            }
            _ => MongodbConnectorError::ConnectionFailure(e),
        })
}

async fn snapshot_collection(
    client: &mongodb::Client,
    db: &mongodb::Database,
    collection: &str,
    table_idx: usize,
    tx: Sender<Result<(usize, Operation), MongodbConnectorError>>,
) -> Result<Timestamp, MongodbConnectorError> {
    let mut session = start_session(client).await?;
    let collection: mongodb::Collection<Document> = db.collection(collection);
    let mut documents = collection
        .find_with_session(None, None, &mut session)
        .await
        .map_err(ConnectionFailure)?;
    let timestamp = session
        .operation_time()
        .expect("Operation time should be `Some` after an operation");
    documents
        .stream(&mut session)
        .map(|doc| {
            let document = doc.map_err(SnapshotReadError)?;
            let id = document_id(&document)?;
            let v: JsonValue =
                serde_json_to_json_value(Bson::Document(document).into_relaxed_extjson())
                    .expect("Could not deserialize bson into json");
            Ok(Operation::Insert {
                new: Record::new(vec![Field::Json(id), Field::Json(v)]),
            })
        })
        .for_each(|op| async {
            tx.send(op.map(|op| (table_idx, op))).await.unwrap();
        })
        .await;
    Ok(timestamp)
}

struct ChangeEventData {
    id: Field,
    fields: Vec<Field>,
}

fn change_event_fields(
    event: &ChangeStreamEvent<Document>,
) -> Result<ChangeEventData, MongodbConnectorError> {
    let id = change_event_id(event)?;
    let doc = event
        .full_document
        .as_ref()
        .expect("No full document on change stream event");
    let serde_json = Bson::from(doc).into_relaxed_extjson();
    let json = serde_json_to_json_value(serde_json).map_err(ReplicationDataError)?;
    Ok(ChangeEventData {
        id: Field::Json(id.clone()),
        fields: vec![Field::Json(id), Field::Json(json)],
    })
}

fn change_event_id(
    event: &ChangeStreamEvent<Document>,
) -> Result<JsonValue, MongodbConnectorError> {
    let key = event
        .document_key
        .as_ref()
        .expect("No document key on change stream event");
    document_id(key)
}

fn document_id(document: &Document) -> Result<JsonValue, MongodbConnectorError> {
    serde_json_to_json_value(
        document
            .get("_id")
            .expect("No _id field in document key")
            .clone()
            .into_relaxed_extjson(),
    )
    .map_err(ReplicationDataError)
}

async fn replicate_collection(
    db: &mongodb::Database,
    collection: &str,
    start_at: Timestamp,
    table_idx: usize,
    tx: Sender<Result<(usize, Operation), MongodbConnectorError>>,
) -> Result<(), MongodbConnectorError> {
    let collection: mongodb::Collection<Document> = db.collection(collection);
    let options = ChangeStreamOptions::builder()
        .start_at_operation_time(Some(start_at))
        // Request the document post-image. This is required, because fine-grained
        // change propagation is not supported for JSON types in dozer
        .full_document(Some(mongodb::options::FullDocumentType::Required))
        .build();
    let events = collection
        .watch(None, Some(options))
        .await
        .map_err(ReplicationError)?;

    events
        .map_err(ReplicationError)
        .and_then(|event| async move {
            match event.operation_type {
                mongodb::change_stream::event::OperationType::Insert => {
                    let data = change_event_fields(&event)?;
                    Ok(Operation::Insert {
                        new: Record::new(data.fields),
                    })
                }
                mongodb::change_stream::event::OperationType::Update
                | mongodb::change_stream::event::OperationType::Replace => {
                    let data = change_event_fields(&event)?;
                    Ok(Operation::Update {
                        old: Record::new(vec![data.id, Field::Null]),
                        new: Record::new(data.fields),
                    })
                }
                mongodb::change_stream::event::OperationType::Delete => {
                    let id = change_event_id(&event)?;
                    Ok(Operation::Delete {
                        old: Record::new(vec![Field::Json(id), Field::Null]),
                    })
                }
                mongodb::change_stream::event::OperationType::Drop
                | mongodb::change_stream::event::OperationType::Rename
                | mongodb::change_stream::event::OperationType::DropDatabase
                | mongodb::change_stream::event::OperationType::Invalidate => {
                    Err(ReplicationStreamInvalidated)
                }
                mongodb::change_stream::event::OperationType::Other(_) => todo!(),
                _ => todo!(),
            }
        })
        .for_each(|op| async { tx.send(op.map(|op| (table_idx, op))).await.unwrap() })
        .await;
    Ok(())
}

#[derive(Default)]
struct ServerInfo {
    replset: bool,
    sharded: bool,
}

impl MongodbConnector {
    pub fn new(connection_string: String) -> Result<Self, MongodbConnectorError> {
        let _ = ConnectionString::parse(&connection_string)
            .map_err(MongodbConnectorError::ParseConnectionString);
        Ok(Self {
            conn_string: connection_string,
        })
    }

    async fn client_options(
        &self,
    ) -> Result<mongodb::options::ClientOptions, MongodbConnectorError> {
        let mut options = ClientOptions::parse(&self.conn_string)
            .await
            .map_err(MongodbConnectorError::ParseConnectionString)?;
        options.write_concern = None;
        Ok(options)
    }

    async fn client(&self) -> Result<mongodb::Client, MongodbConnectorError> {
        let options = self.client_options().await?;
        self.client_with_options(options).await
    }

    async fn client_with_options(
        &self,
        options: mongodb::options::ClientOptions,
    ) -> Result<mongodb::Client, MongodbConnectorError> {
        let client = mongodb::Client::with_options(options).unwrap();
        if client.default_database().is_none() {
            return Err(NoDatabaseError);
        }
        Ok(client)
    }

    fn database(&self, client: &mongodb::Client) -> mongodb::Database {
        client
            .default_database()
            .expect("No default database specified")
    }

    async fn identify_server(
        &self,
        client: &mongodb::Client,
    ) -> Result<ServerInfo, MongodbConnectorError> {
        let db = self.database(client);
        let hello = doc! {
            "hello": 1,
        };
        // This command should always succeed, if we can connect. So, on error, connecting failed.
        let hello_result = db
            .run_command(hello, None)
            .await
            .map_err(MongodbConnectorError::ConnectionFailure)?;

        let mut server_info = ServerInfo::default();

        // This field is only present if the instance is a member of a replset
        if hello_result.get("setName").is_some() {
            server_info.replset = true;
        }

        if let Ok("isdbgrid") = hello_result.get_str("msg") {
            server_info.sharded = true;
        }

        Ok(server_info)
    }

    async fn validate_table_privileges(
        &self,
        database: &mongodb::Database,
        username: &str,
        tables: &[TableIdentifier],
    ) -> Result<(), MongodbConnectorError> {
        // Users can always view their own privileges, so failure here is a connection
        // error
        let user_info = database
            .run_command(
                Document::from_iter([
                    ("usersInfo".to_owned(), username.into()),
                    ("showPrivileges".to_owned(), true.into()),
                ]),
                None,
            )
            .await
            .map_err(ConnectionFailure)?;
        let privileges = user_info.get_array("users").unwrap()[0]
            .as_document()
            .unwrap()
            .get_array("inheritedPrivileges")
            .unwrap()
            .iter()
            .filter_map(|privilege| privilege.as_document());

        let mut table_privs: HashMap<&str, Privs> = HashMap::with_capacity(tables.len());
        for table in tables {
            table_privs.insert(
                &table.name,
                Privs {
                    find: false,
                    watch: false,
                },
            );
        }

        let mut db_or_global_privs = Privs {
            find: false,
            watch: false,
        };
        // We need the `find` and `changeStream` privileges for all collections,
        // or for the entire database, or for the entire server

        for privilege in privileges {
            let Ok(actions) = privilege.get_array("actions") else {
                continue;
            };

            let Some((db, collection)) =
                privilege
                    .get_document("resource")
                    .ok()
                    .and_then(|resource| {
                        let Ok(db) = resource.get_str("db") else {
                            return None;
                        };
                        let Ok(collection) = resource.get_str("collection") else {
                            return None;
                        };
                        Some((db, collection))
                    })
            else {
                continue;
            };

            let mut privs = Privs {
                find: false,
                watch: false,
            };

            for action in actions {
                if action.as_str() == Some("find") {
                    privs.find = true;
                }

                if action.as_str() == Some("changeStream") {
                    privs.watch = true;
                }
            }

            if db.is_empty() || collection.is_empty() {
                db_or_global_privs |= privs;
            } else if db == database.name() {
                if let Some(table_priv) = table_privs.get_mut(collection) {
                    *table_priv |= privs;
                }
            }
        }

        if db_or_global_privs.find && db_or_global_privs.watch {
            return Ok(());
        }

        let mut missing_privs = Vec::new();
        for table in tables {
            let privs = table_privs
                .get(table.name.as_str())
                .copied()
                .unwrap_or_default()
                | db_or_global_privs;

            let mut missing_table_privs = Vec::new();
            if !privs.find {
                missing_table_privs.push("find".to_owned());
            }
            if !privs.watch {
                missing_table_privs.push("changeStream".to_owned());
            }

            if !missing_table_privs.is_empty() {
                missing_privs.push((table.name.to_owned(), missing_table_privs));
            }
        }
        if missing_privs.is_empty() {
            Ok(())
        } else {
            Err(MissingPermissions(missing_privs))
        }
    }
}

#[async_trait]
impl Connector for MongodbConnector {
    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        let client = self.client().await?;
        let server_info = self.identify_server(&client).await?;
        if !server_info.replset {
            return Err(NotAReplicaSet.into());
        }
        if server_info.sharded {
            return Err(Sharded.into());
        }
        Ok(())
    }

    fn types_mapping() -> Vec<(String, Option<FieldType>)> {
        todo!();
    }

    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        Ok(tables
            .into_iter()
            .map(|table| TableInfo {
                schema: None,
                name: table.name,
                column_names: vec!["data".to_owned()],
            })
            .collect())
    }

    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let _ = self.client().await?;
        Ok(table_infos
            .iter()
            .map(|_table_info| {
                Ok(SourceSchema {
                    schema: dozer_types::types::Schema {
                        fields: vec![
                            FieldDefinition {
                                name: "_id".to_owned(),
                                typ: FieldType::Json,
                                nullable: false,
                                source: SourceDefinition::Dynamic,
                            },
                            FieldDefinition {
                                name: "data".to_owned(),
                                typ: FieldType::Json,
                                nullable: false,
                                source: SourceDefinition::Dynamic,
                            },
                        ],
                        primary_index: vec![0],
                    },
                    cdc_type: CdcType::OnlyPK,
                })
            })
            .collect())
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let client = self.client().await?;
        let database = self.database(&client);
        let collections = database
            .list_collection_names(None)
            .await
            .map_err(ListTablesError)?;

        dozer_types::log::debug!("Collections: {:?}", &collections);

        Ok(database
            .list_collection_names(None)
            .await
            .map_err(ListTablesError)?
            .into_iter()
            .map(|collection_name| TableIdentifier::new(None, collection_name))
            .collect())
    }

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let options = self.client_options().await?;
        let client = self.client_with_options(options.clone()).await?;
        let database = self.database(&client);
        let user = options
            .credential
            .as_ref()
            .and_then(|cred| cred.username.as_ref());

        // If we could connect without a user, there is no access control and
        // we can do whatever we want. Else, check whether we have the correct privileges
        // for replication
        if let Some(user) = user {
            self.validate_table_privileges(&database, user, tables)
                .await?;
        }

        let table_names = tables
            .iter()
            .map(|table| Bson::String(table.name.clone()))
            .collect::<Vec<Bson>>();
        let collection_list = database
            .list_collections(Some(doc! {"name": {"$in": table_names}}), None)
            .await;
        // Try to check whether the collection is capped (capped collections can't be watched),
        // and whether pre- and post-images are enabled (these are currently needed to
        // get the result of updates). This needs the `listCollections` privilege,
        // which is included in the standard `read` role. If we don't have this privilege,
        // we succeed for now, but might fail when starting replication.
        if let Ok(collections) = collection_list {
            collections
                .map_err(MongodbConnectorError::ConnectionFailure)
                .try_for_each(|collection_info| async {
                    let options = collection_info.options;
                    if options.capped.unwrap_or(false) {
                        return Err(CappedCollection(collection_info.name));
                    }

                    if !options
                        .change_stream_pre_and_post_images
                        .map(|option| option.enabled)
                        .unwrap_or(false)
                    {
                        return Err(NoPrePostImages(collection_info.name));
                    }

                    Ok::<_, MongodbConnectorError>(())
                })
                .await?;
        }
        Ok(())
    }

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        // Snapshot: find
        //
        // Replicate: changeStream
        let client = self.client().await?;
        let database = self.database(&client);

        let (tx, mut rx) = channel::<Result<(usize, Operation), MongodbConnectorError>>(100);

        let snapshots = FuturesUnordered::new();
        for (idx, table) in tables.iter().enumerate() {
            let fut = snapshot_collection(&client, &database, &table.name, idx, tx.clone())
                .map_ok(move |timestamp| (idx, timestamp));
            snapshots.push(fut);
        }
        drop(tx);

        let snapshot_ingestor = ingestor.clone();
        let snapshot_task = tokio::spawn(async move {
            if snapshot_ingestor
                .handle_message(IngestionMessage::SnapshottingStarted)
                .await
                .is_err()
            {
                // If the ingestor is already closed, we don't need to do anything
                return Ok::<_, MongodbConnectorError>(());
            }
            while let Some(result) = rx.recv().await {
                let (table_index, op) = result?;
                if snapshot_ingestor
                    .handle_message(IngestionMessage::OperationEvent {
                        table_index,
                        op,
                        id: None,
                    })
                    .await
                    .is_err()
                {
                    // If the ingestor is already closed, we don't need to do anything
                    return Ok(());
                }
            }
            if snapshot_ingestor
                .handle_message(IngestionMessage::SnapshottingDone { id: None })
                .await
                .is_err()
            {
                // If the ingestor is already closed, we don't need to do anything
                return Ok(());
            };
            Ok(())
        });

        let timestamps: Vec<(usize, Timestamp)> = snapshots.try_collect().await?;

        snapshot_task.await.unwrap()?;

        let (tx, mut rx) = channel::<Result<(usize, Operation), MongodbConnectorError>>(100);

        let replicators = FuturesUnordered::new();
        for (table_idx, timestamp) in timestamps {
            let tx = tx.clone();
            replicators.push(replicate_collection(
                &database,
                &tables[table_idx].name,
                timestamp,
                table_idx,
                tx,
            ));
        }
        drop(tx);

        let ingestor = ingestor.clone();
        let replication_task = tokio::spawn(async move {
            while let Some(result) = rx.recv().await {
                let (table_index, op) = result?;
                if ingestor
                    .handle_message(IngestionMessage::OperationEvent {
                        table_index,
                        op,
                        id: None,
                    })
                    .await
                    .is_err()
                {
                    // If the ingestor is already closed, we don't need to do anything
                    return Ok::<_, MongodbConnectorError>(());
                }
            }
            Ok(())
        });

        let _: () = replicators.try_collect().await?;
        let _ = replication_task.await.unwrap()?;
        Ok(())
    }
}
