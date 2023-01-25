use crate::connectors::postgres::helper;
use crate::errors::{PostgresConnectorError, PostgresSchemaError};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::types::{
    Field, FieldDefinition, Operation, OperationEvent, Record, Schema, SourceDefinition,
};
use helper::postgres_type_to_dozer_type;
use postgres_protocol::message::backend::LogicalReplicationMessage::{
    Begin, Commit, Delete, Insert, Relation, Update,
};
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, RelationBody, ReplicaIdentity, TupleData, UpdateBody, XLogDataBody,
};
use postgres_types::Type;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

struct MessageBody<'a> {
    message: &'a RelationBody,
}

impl<'a> MessageBody<'a> {
    pub fn new(message: &'a RelationBody) -> Self {
        Self { message }
    }
}

#[derive(Debug)]
pub struct Table {
    columns: Vec<TableColumn>,
    hash: u64,
    rel_id: u32,
    replica_identity: ReplicaIdentity,
}

#[derive(Debug)]
pub struct TableColumn {
    pub name: String,
    pub type_id: i32,
    pub flags: i8,
    pub r#type: Option<Type>,
    pub idx: usize,
}

impl Hash for MessageBody<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let columns_vec: Vec<(i32, &str, i8)> = self
            .message
            .columns()
            .iter()
            .map(|column| (column.type_id(), column.name().unwrap(), column.flags()))
            .collect();

        columns_vec.hash(state);
    }
}

pub struct XlogMapper {
    relations_map: HashMap<u32, Table>,
    tables_columns: HashMap<u32, Vec<String>>,
}

impl Default for XlogMapper {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl XlogMapper {
    pub fn new(tables_columns: HashMap<u32, Vec<String>>) -> Self {
        XlogMapper {
            relations_map: HashMap::<u32, Table>::new(),
            tables_columns,
        }
    }

    pub fn handle_message(
        &mut self,
        message: XLogDataBody<LogicalReplicationMessage>,
    ) -> Result<Option<IngestionMessage>, PostgresConnectorError> {
        match &message.data() {
            Relation(relation) => {
                let body = MessageBody::new(relation);
                let mut s = DefaultHasher::new();
                body.hash(&mut s);
                let hash = s.finish();

                let table_option = self.relations_map.get(&relation.rel_id());
                match table_option {
                    None => {
                        self.ingest_schema(relation, hash)?;
                    }
                    Some(table) => {
                        if table.hash != hash {
                            self.ingest_schema(relation, hash)?;
                        }
                    }
                }
            }
            Commit(commit) => {
                return Ok(Some(IngestionMessage::Commit(dozer_types::types::Commit {
                    seq_no: 0,
                    lsn: commit.end_lsn(),
                })));
            }
            Begin(_begin) => {
                return Ok(Some(IngestionMessage::Begin()));
            }
            Insert(insert) => {
                let table = self.relations_map.get(&insert.rel_id()).unwrap();
                let new_values = insert.tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values, false)?;

                let event = OperationEvent {
                    operation: Operation::Insert {
                        new: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id,
                                version: table.rel_id as u16,
                            }),
                            values,
                            None,
                        ),
                    },
                    seq_no: 0,
                };

                return Ok(Some(IngestionMessage::OperationEvent(event)));
            }
            Update(update) => {
                let table = self.relations_map.get(&update.rel_id()).unwrap();
                let new_values = update.new_tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values, false)?;
                let old_values = Self::convert_old_value_to_fields(table, update)?;

                let event = OperationEvent {
                    operation: Operation::Update {
                        old: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id,
                                version: table.rel_id as u16,
                            }),
                            old_values,
                            None,
                        ),
                        new: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id,
                                version: table.rel_id as u16,
                            }),
                            values,
                            None,
                        ),
                    },
                    seq_no: 0,
                };

                return Ok(Some(IngestionMessage::OperationEvent(event)));
            }
            Delete(delete) => {
                // TODO: Use only columns with .flags() = 0
                let table = self.relations_map.get(&delete.rel_id()).unwrap();
                let key_values = delete.key_tuple().unwrap().tuple_data();

                let values = Self::convert_values_to_fields(table, key_values, true)?;

                let event = OperationEvent {
                    operation: Operation::Delete {
                        old: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id,
                                version: table.rel_id as u16,
                            }),
                            values,
                            None,
                        ),
                    },
                    seq_no: 0,
                };

                return Ok(Some(IngestionMessage::OperationEvent(event)));
            }
            _ => {}
        }

        Ok(None)
    }

    fn ingest_schema(
        &mut self,
        relation: &RelationBody,
        hash: u64,
    ) -> Result<(), PostgresConnectorError> {
        let rel_id = relation.rel_id();
        let existing_columns = self
            .tables_columns
            .get(&rel_id)
            .map_or(vec![], |t| t.clone());

        let columns: Vec<TableColumn> = relation
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                existing_columns.is_empty()
                    || existing_columns.contains(&column.name().unwrap().to_string())
            })
            .map(|(idx, column)| TableColumn {
                name: String::from(column.name().unwrap()),
                type_id: column.type_id(),
                flags: column.flags(),
                r#type: Type::from_oid(column.type_id() as u32),
                idx,
            })
            .collect();

        let _table_name = relation
            .name()
            .map_err(PostgresConnectorError::RelationNotFound)?
            .to_string();

        let replica_identity = match relation.replica_identity() {
            ReplicaIdentity::Default => ReplicaIdentity::Default,
            ReplicaIdentity::Nothing => ReplicaIdentity::Nothing,
            ReplicaIdentity::Full => ReplicaIdentity::Full,
            ReplicaIdentity::Index => ReplicaIdentity::Index,
        };

        let table = Table {
            columns,
            hash,
            rel_id,
            replica_identity,
        };

        let mut fields = vec![];
        for c in &table.columns {
            let typ = c.r#type.clone();
            let typ = typ
                .map_or(
                    Err(PostgresSchemaError::InvalidColumnType),
                    postgres_type_to_dozer_type,
                )
                .map_err(PostgresConnectorError::PostgresSchemaError)?;

            fields.push(FieldDefinition {
                name: c.name.clone(),
                typ,
                nullable: true,
                source: SourceDefinition::Dynamic,
            });
        }

        let _schema = Schema {
            identifier: Some(dozer_types::types::SchemaIdentifier {
                id: table.rel_id,
                version: table.rel_id as u16,
            }),
            fields,
            primary_index: vec![0],
        };

        self.relations_map.insert(rel_id, table);

        Ok(())
    }

    fn convert_values_to_fields(
        table: &Table,
        new_values: &[TupleData],
        only_key: bool,
    ) -> Result<Vec<Field>, PostgresConnectorError> {
        let mut values: Vec<Field> = vec![];

        for column in &table.columns {
            if column.flags == 1 || !only_key {
                let value = new_values.get(column.idx).unwrap();
                match value {
                    TupleData::Null => values.push(
                        helper::postgres_type_to_field(None, column)
                            .map_err(PostgresConnectorError::PostgresSchemaError)?,
                    ),
                    TupleData::UnchangedToast => {}
                    TupleData::Text(text) => values.push(
                        helper::postgres_type_to_field(Some(text), column)
                            .map_err(PostgresConnectorError::PostgresSchemaError)?,
                    ),
                }
            } else {
                values.push(Field::Null);
            }
        }

        Ok(values)
    }

    fn convert_old_value_to_fields(
        table: &Table,
        update: &UpdateBody,
    ) -> Result<Vec<Field>, PostgresConnectorError> {
        match table.replica_identity {
            ReplicaIdentity::Default | ReplicaIdentity::Full | ReplicaIdentity::Index => {
                update.key_tuple().map_or_else(
                    || Self::convert_values_to_fields(table, update.new_tuple().tuple_data(), true),
                    |key_tuple| Self::convert_values_to_fields(table, key_tuple.tuple_data(), true),
                )
            }
            ReplicaIdentity::Nothing => Ok(vec![]),
        }
    }
}
