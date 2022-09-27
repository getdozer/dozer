use crate::connectors::ingestor::IngestionMessage;
use crate::connectors::postgres::helper;
use dozer_types::types::{Field, Operation, OperationEvent, Record, Schema};
use postgres_protocol::message::backend::LogicalReplicationMessage::{
    Begin, Commit, Delete, Insert, Relation, Update,
};
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, RelationBody, TupleData, XLogDataBody,
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

pub struct Table {
    name: String,
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
            .into_iter()
            .map(|column| (column.type_id(), column.name().unwrap(), column.flags()))
            .collect();

        columns_vec.hash(state);
    }
}

pub struct XlogMapper {
    relations_map: HashMap<u32, Table>,
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
    ) -> Option<IngestionMessage> {
        match &message.data() {
            Relation(relation) => {
                println!("relation:");
                println!("[Relation] Rel ID: {}", relation.rel_id());
                println!("[Relation] Rel columns: {:?}", relation.columns());

                let body = MessageBody::new(&relation);
                let mut s = DefaultHasher::new();
                body.hash(&mut s);
                let hash = s.finish();

                let table_option = self.relations_map.get(&relation.rel_id());
                match table_option {
                    None => {
                        let schema_change = self.ingest_schema(relation, hash);
                        return Option::from(schema_change);
                    }
                    Some(table) => {
                        if table.hash != hash {
                            let schema_change = self.ingest_schema(relation, hash);
                            return Option::from(schema_change);
                        }
                    }
                }
            }
            Commit(commit) => {
                println!("commit:");
                println!("[Commit] End lsn: {}", commit.end_lsn());

                return Option::from(IngestionMessage::Commit());
            }
            Begin(begin) => {
                println!("begin:");
                println!("[Begin] Transaction id: {}", begin.xid());
                return Option::from(IngestionMessage::Begin());
            }
            Insert(insert) => {
                let table = self.relations_map.get(&insert.rel_id()).unwrap();
                let new_values = insert.tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values);

                let event = OperationEvent {
                    operation: Operation::Insert {
                        table_name: table.name.clone(),
                        new: Record {
                            values,
                            schema_id: table.rel_id as u64,
                        },
                    },
                    id: 0,
                };

                return Option::from(IngestionMessage::OperationEvent(event));
            }
            Update(update) => {
                let table = self.relations_map.get(&update.rel_id()).unwrap();
                let new_values = update.new_tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values);
                let event = OperationEvent {
                    operation: Operation::Update {
                        table_name: table.name.clone(),
                        old: Record {
                            values: vec![],
                            schema_id: table.rel_id as u64,
                        },
                        new: Record {
                            values,
                            schema_id: table.rel_id as u64,
                        },
                    },
                    id: 0,
                };

                return Option::from(IngestionMessage::OperationEvent(event));
            }
            Delete(delete) => {
                // TODO: Use only columns with .flags() = 0
                let table = self.relations_map.get(&delete.rel_id()).unwrap();
                let key_values = delete.key_tuple().unwrap().tuple_data();

                let values = Self::convert_values_to_fields(table, key_values);

                let event = OperationEvent {
                    operation: Operation::Delete {
                        table_name: table.name.clone(),
                        old: Record {
                            values,
                            schema_id: table.rel_id as u64,
                        },
                    },
                    id: 0,
                };

                return Option::from(IngestionMessage::OperationEvent(event));
            }
            _ => {}
        }

        return None;
    }

    fn ingest_schema(&mut self, relation: &RelationBody, hash: u64) -> IngestionMessage {
        let rel_id = relation.rel_id();
        let columns: Vec<TableColumn> = relation
            .columns()
            .into_iter()
            .map(|column| TableColumn {
                name: String::from(column.name().unwrap()),
                type_id: column.type_id(),
                flags: column.flags(),
                r#type: Type::from_oid(column.type_id() as u32),
            })
            .collect();

        let table = Table {
            name: String::from(relation.name().unwrap()),
            columns,
            hash,
            rel_id,
        };

        let schema = Schema {
            id: table.rel_id.to_string(),
            field_names: table
                .columns
                .iter()
                .map(|column| column.name.to_string())
                .collect(),
            field_types: table
                .columns
                .iter()
                .map(|column| helper::postgres_type_to_dozer_type(&column))
                .collect(),
            _idx: Default::default(),
            _ctr: 0,
        };

        self.relations_map.insert(rel_id, table);

        IngestionMessage::Schema(schema)
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
