use crate::connectors::ingestor::IngestionMessage;
use crate::connectors::postgres::helper;
use dozer_types::errors::connector::ConnectorError;
use dozer_types::log::debug;
use dozer_types::types::{Field, FieldDefinition, Operation, OperationEvent, Record, Schema};
use helper::postgres_type_to_dozer_type;
use postgres_protocol::message::backend::LogicalReplicationMessage::{
    Begin, Commit, Delete, Insert, Relation, Update,
};
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, RelationBody, TupleData, XLogDataBody,
};
use postgres_types_materialize::Type;
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

pub struct Table {
    columns: Vec<TableColumn>,
    hash: u64,
    rel_id: u32,
}

pub struct TableColumn {
    pub name: String,
    pub type_id: i32,
    pub flags: i8,
    pub r#type: Option<Type>,
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
}

impl Default for XlogMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl XlogMapper {
    pub fn new() -> Self {
        XlogMapper {
            relations_map: HashMap::<u32, Table>::new(),
        }
    }

    pub fn handle_message(
        &mut self,
        message: XLogDataBody<LogicalReplicationMessage>,
    ) -> Result<Option<IngestionMessage>, ConnectorError> {
        match &message.data() {
            Relation(relation) => {
                debug!("relation:");
                debug!("[Relation] Rel ID: {}", relation.rel_id());
                debug!("[Relation] Rel columns: {:?}", relation.columns());

                let body = MessageBody::new(relation);
                let mut s = DefaultHasher::new();
                body.hash(&mut s);
                let hash = s.finish();

                let table_option = self.relations_map.get(&relation.rel_id());
                match table_option {
                    None => {
                        return self.ingest_schema(relation, hash);
                    }
                    Some(table) => {
                        if table.hash != hash {
                            return self.ingest_schema(relation, hash);
                        }
                    }
                }
            }
            Commit(commit) => {
                debug!("commit:");
                debug!("[Commit] End lsn: {}", commit.end_lsn());

                return Ok(Some(IngestionMessage::Commit(dozer_types::types::Commit {
                    seq_no: 0,
                    lsn: commit.end_lsn(),
                })));
            }
            Begin(begin) => {
                debug!("begin:");
                debug!("[Begin] Transaction id: {}", begin.xid());
                return Ok(Some(IngestionMessage::Begin()));
            }
            Insert(insert) => {
                let table = self.relations_map.get(&insert.rel_id()).unwrap();
                let new_values = insert.tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values);

                let event = OperationEvent {
                    operation: Operation::Insert {
                        new: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id as u32,
                                version: table.rel_id as u16,
                            }),
                            values,
                        ),
                    },
                    seq_no: 0,
                };

                return Ok(Some(IngestionMessage::OperationEvent(event)));
            }
            Update(update) => {
                let table = self.relations_map.get(&update.rel_id()).unwrap();
                let new_values = update.new_tuple().tuple_data();

                debug!("old tuple: {:?}", update.old_tuple());
                debug!("key tuple: {:?}", update.key_tuple());
                let values = Self::convert_values_to_fields(table, new_values);
                let old_values = if let Some(key_values) = update.key_tuple() {
                    Self::convert_values_to_fields(table, key_values.tuple_data())
                } else {
                    vec![]
                };

                let event = OperationEvent {
                    operation: Operation::Update {
                        old: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id as u32,
                                version: table.rel_id as u16,
                            }),
                            old_values,
                        ),
                        new: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id as u32,
                                version: table.rel_id as u16,
                            }),
                            values,
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

                let values = Self::convert_values_to_fields(table, key_values);

                let event = OperationEvent {
                    operation: Operation::Delete {
                        old: Record::new(
                            Some(dozer_types::types::SchemaIdentifier {
                                id: table.rel_id as u32,
                                version: table.rel_id as u16,
                            }),
                            values,
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
    ) -> Result<Option<IngestionMessage>, ConnectorError> {
        let rel_id = relation.rel_id();
        let columns: Vec<TableColumn> = relation
            .columns()
            .iter()
            .map(|column| TableColumn {
                name: String::from(column.name().unwrap()),
                type_id: column.type_id(),
                flags: column.flags(),
                r#type: Type::from_oid(column.type_id() as u32),
            })
            .collect();

        let table = Table {
            columns,
            hash,
            rel_id,
        };

        let fields: Result<Vec<FieldDefinition>, _> = table
            .columns
            .iter()
            .map(|c| match postgres_type_to_dozer_type(c.r#type.clone()) {
                Ok(typ) => Ok(FieldDefinition {
                    name: c.name.clone(),
                    typ,
                    nullable: true,
                }),
                Err(e) => Err(e),
            })
            .collect();

        let schema = Schema {
            identifier: Some(dozer_types::types::SchemaIdentifier {
                id: table.rel_id as u32,
                version: table.rel_id as u16,
            }),
            fields: fields?,
            values: vec![0],
            primary_index: vec![0],
            secondary_indexes: vec![],
        };

        self.relations_map.insert(rel_id, table);

        Ok(Some(IngestionMessage::Schema(schema)))
    }

    fn convert_values_to_fields(table: &Table, new_values: &[TupleData]) -> Vec<Field> {
        let mut values: Vec<Field> = vec![];

        for i in 0..new_values.len() {
            let value = new_values.get(i).unwrap();
            let column = table.columns.get(i).unwrap();
            if let TupleData::Text(text) = value {
                values.push(helper::postgres_type_to_field(text, column));
            }
        }

        values
    }
}
