use crate::connectors::postgres::helper;
use dozer_shared::types::{Operation, OperationEvent};
use dozer_storage::storage::RocksStorage;
use postgres_protocol::message::backend::LogicalReplicationMessage::{
    Begin, Commit, Delete, Insert, Origin, Relation, Type, Update,
};
use postgres_protocol::message::backend::{
    Column, LogicalReplicationMessage, TupleData, XLogDataBody,
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct XlogMapper {
    messages_buffer: Vec<XLogDataBody<LogicalReplicationMessage>>,
}

impl XlogMapper {
    pub fn new() -> Self {
        XlogMapper {
            messages_buffer: vec![],
        }
    }

    pub async fn handle_message(
        &mut self,
        message: XLogDataBody<LogicalReplicationMessage>,
        storage_client: Arc<RocksStorage>,
    ) {
        match message.data() {
            Insert(insert) => {
                println!("insert:");
                println!("{:?}", insert.tuple().tuple_data());
            }
            Update(update) => {
                println!("update:");
                println!("[Update] REL ID: {}", update.rel_id());
                println!("[Update] REL ID: {:?}", update.new_tuple());
            }
            Delete(delete) => {
                println!("delete:");
                println!("[Delete] REL ID: {}", delete.rel_id());
            }
            Begin(begin) => {
                println!("begin:");
                println!("[Begin] Transaction id: {}", begin.xid());
                self.messages_buffer = vec![];
            }
            Commit(commit) => {
                println!("commit:");
                println!("[Commit] End lsn: {}", commit.end_lsn());
                let operation_events = self.map_operation_events();
                helper::insert_operation_events(storage_client, operation_events).await;
            }
            Relation(relation) => {
                println!("relation:");
                println!("[Relation] Rel ID: {}", relation.rel_id());
                println!("[Relation] Rel columns: {:?}", relation.columns());
            }
            Origin(origin) => {
                println!("origin: {:?}", origin);
            }
            Type(typ) => {
                println!("type: {:?}", typ);
            }
            _ => {
                panic!("Why is this happening")
            }
        }

        self.messages_buffer.push(message);
    }

    fn convert_values_to_vec(columns: &&[Column], new_values: &[TupleData]) -> Vec<Vec<u8>> {
        let mut values: Vec<Vec<u8>> = vec![];

        for i in 0..new_values.len() {
            let value = new_values.get(i).unwrap();
            let column = columns.get(i).unwrap();
            if let TupleData::Text(text) = value {
                values.push(helper::postgres_type_to_bytes(text, column));
            }
        }

        values
    }

    fn map_operation_events(&mut self) -> Vec<OperationEvent> {
        // let mut relations_map = HashMap::<u32, &[Column]>::new();
        // for message in self.messages_buffer.iter() {
        //     if let Relation(relation) = message.data() {
        //         relations_map.insert(relation.rel_id(), relation.columns());
        //     }
        // }

        // let mut operations: Vec<OperationEvent> = vec![];
        // for message in self.messages_buffer.iter() {
        //     match message.data() {
        //         Insert(insert) => {
        //             let columns = relations_map.get(&insert.rel_id()).unwrap();
        //             let new_values = insert.tuple().tuple_data();

        //             let values = Self::convert_values_to_vec(columns, new_values);

        //             operations.push(Operation {
        //                 operation_type: "insert".to_string(),
        //                 schema_id: insert.rel_id(),
        //                 values,
        //             });
        //         }
        //         Update(update) => {
        //             let columns = relations_map.get(&update.rel_id()).unwrap();
        //             let new_values = update.new_tuple().tuple_data();

        //             let values = Self::convert_values_to_vec(columns, new_values);

        //             operations.push(Operation {
        //                 operation_type: "update".to_string(),
        //                 schema_id: update.rel_id(),
        //                 values,
        //             });
        //         }
        //         Delete(delete) => {
        //             // TODO: Use only columns with .flags() = 0
        //             let columns = relations_map.get(&delete.rel_id()).unwrap();
        //             let key_values = delete.key_tuple().unwrap().tuple_data();

        //             let values = Self::convert_values_to_vec(columns, key_values);

        //             operations.push(Operation {
        //                 operation_type: "delete".to_string(),
        //                 schema_id: delete.rel_id(),
        //                 values,
        //             })
        //         }
        //         _ => {}
        //     }
        // }

        // operations
        vec![]
    }
}
