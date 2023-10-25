use dozer_ingestion_connector::dozer_types::{
    node::OpIdentifier,
    types::{Field, Operation, Record},
};
use postgres_protocol::message::backend::LogicalReplicationMessage::{
    Begin, Commit, Delete, Insert, Relation, Update,
};
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, RelationBody, ReplicaIdentity, TupleData, UpdateBody, XLogDataBody,
};
use postgres_types::Type;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use crate::{
    helper::{self, postgres_type_to_dozer_type},
    PostgresConnectorError, PostgresSchemaError,
};

#[derive(Debug)]
pub struct Table {
    columns: Vec<TableColumn>,
    replica_identity: ReplicaIdentity,
}

#[derive(Debug)]
pub struct TableColumn {
    pub name: String,
    pub flags: i8,
    pub r#type: Type,
    pub column_index: usize,
}

#[derive(Debug, Clone)]
pub enum MappedReplicationMessage {
    Begin,
    Commit(OpIdentifier),
    Operation { table_index: usize, op: Operation },
}

#[derive(Debug, Default)]
pub struct XlogMapper {
    /// Relation id to table info from replication `Relation` message.
    relations_map: HashMap<u32, Table>,
    /// Relation id to (table index, column names).
    tables_columns: HashMap<u32, (usize, Vec<String>)>,
}

impl XlogMapper {
    pub fn new(tables_columns: HashMap<u32, (usize, Vec<String>)>) -> Self {
        XlogMapper {
            relations_map: HashMap::<u32, Table>::new(),
            tables_columns,
        }
    }

    pub fn handle_message(
        &mut self,
        message: XLogDataBody<LogicalReplicationMessage>,
    ) -> Result<Option<MappedReplicationMessage>, PostgresConnectorError> {
        match &message.data() {
            Relation(relation) => {
                self.ingest_schema(relation)?;
            }
            Commit(commit) => {
                return Ok(Some(MappedReplicationMessage::Commit(OpIdentifier::new(
                    commit.end_lsn(),
                    0,
                ))));
            }
            Begin(_begin) => {
                return Ok(Some(MappedReplicationMessage::Begin));
            }
            Insert(insert) => {
                let Some(table_columns) = self.tables_columns.get(&insert.rel_id()) else {
                    return Ok(None);
                };
                let table_index = table_columns.0;

                let table = self.relations_map.get(&insert.rel_id()).unwrap();
                let new_values = insert.tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values, false)?;

                let event = Operation::Insert {
                    new: Record::new(values),
                };

                return Ok(Some(MappedReplicationMessage::Operation {
                    table_index,
                    op: event,
                }));
            }
            Update(update) => {
                let Some(table_columns) = self.tables_columns.get(&update.rel_id()) else {
                    return Ok(None);
                };
                let table_index = table_columns.0;

                let table = self.relations_map.get(&update.rel_id()).unwrap();
                let new_values = update.new_tuple().tuple_data();

                let values = Self::convert_values_to_fields(table, new_values, false)?;
                let old_values = Self::convert_old_value_to_fields(table, update)?;

                let event = Operation::Update {
                    old: Record::new(old_values),
                    new: Record::new(values),
                };

                return Ok(Some(MappedReplicationMessage::Operation {
                    table_index,
                    op: event,
                }));
            }
            Delete(delete) => {
                let Some(table_columns) = self.tables_columns.get(&delete.rel_id()) else {
                    return Ok(None);
                };
                let table_index = table_columns.0;

                // TODO: Use only columns with .flags() = 0
                let table = self.relations_map.get(&delete.rel_id()).unwrap();
                let key_values = delete.key_tuple().unwrap().tuple_data();

                let values = Self::convert_values_to_fields(table, key_values, true)?;

                let event = Operation::Delete {
                    old: Record::new(values),
                };

                return Ok(Some(MappedReplicationMessage::Operation {
                    table_index,
                    op: event,
                }));
            }
            _ => {}
        }

        Ok(None)
    }

    fn ingest_schema(&mut self, relation: &RelationBody) -> Result<(), PostgresConnectorError> {
        let rel_id = relation.rel_id();
        let Some((table_index, wanted_columns)) = self.tables_columns.get(&rel_id) else {
            return Ok(());
        };

        let mut columns = vec![];
        for (column_index, column) in relation.columns().iter().enumerate() {
            let column_name =
                column
                    .name()
                    .map_err(|_| PostgresConnectorError::NonUtf8ColumnName {
                        table_index: *table_index,
                        column_index,
                    })?;

            if !wanted_columns.is_empty()
                && !wanted_columns
                    .iter()
                    .any(|column| column.as_str() == column_name)
            {
                continue;
            }

            // TODO: workaround - in case of custom enum
            let type_oid = column.type_id() as u32;
            let typ = if type_oid == 28862 {
                Type::VARCHAR
            } else {
                Type::from_oid(type_oid).ok_or_else(|| {
                    PostgresSchemaError::InvalidColumnType(column_name.to_string())
                })?
            };

            columns.push(TableColumn {
                name: column_name.to_string(),
                flags: column.flags(),
                r#type: typ,
                column_index,
            })
        }

        columns.sort_by_cached_key(|column| {
            wanted_columns
                .iter()
                .position(|wanted| wanted == &column.name)
                // Unwrap is safe because we filtered on present keys above
                .unwrap()
        });

        let replica_identity = match relation.replica_identity() {
            ReplicaIdentity::Default => ReplicaIdentity::Default,
            ReplicaIdentity::Nothing => ReplicaIdentity::Nothing,
            ReplicaIdentity::Full => ReplicaIdentity::Full,
            ReplicaIdentity::Index => ReplicaIdentity::Index,
        };

        let table = Table {
            columns,
            replica_identity,
        };

        for c in &table.columns {
            postgres_type_to_dozer_type(c.r#type.clone())?;
        }

        match self.relations_map.entry(rel_id) {
            Entry::Occupied(mut entry) => {
                // Check if type has changed.
                for (existing_column, column) in entry.get().columns.iter().zip(&table.columns) {
                    if existing_column.r#type != column.r#type {
                        return Err(PostgresConnectorError::ColumnTypeChanged {
                            table_index: *table_index,
                            column_name: existing_column.name.clone(),
                            old_type: existing_column.r#type.clone(),
                            new_type: column.r#type.clone(),
                        });
                    }
                }

                entry.insert(table);
            }
            Entry::Vacant(entry) => {
                entry.insert(table);
            }
        }

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
                let value = new_values.get(column.column_index).unwrap();
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
