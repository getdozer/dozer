use super::{helper, iterator::CacheIterator};
use crate::cache::{
    expression::{ExecutionStep, IndexScan, QueryExpression},
    index,
    planner::QueryPlanner,
};
use dozer_types::errors::{
    cache::{
        CacheError::{self},
        IndexError, QueryError,
    },
    types::TypeError,
};
use dozer_types::{
    bincode, json_value_to_field,
    types::{Field, Record, Schema},
};
use galil_seiferas::gs_find;
use lmdb::{Database, RoTransaction, Transaction};

pub struct LmdbQueryHandler<'a> {
    db: Database,
    indexer_db: Database,
    txn: &'a RoTransaction<'a>,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(db: Database, indexer_db: Database, txn: &'a RoTransaction) -> Self {
        Self {
            db,
            indexer_db,
            txn,
        }
    }

    pub fn query(
        &self,
        schema: &Schema,
        query: &QueryExpression,
    ) -> Result<Vec<Record>, CacheError> {
        let planner = QueryPlanner {};
        let execution = planner.plan(schema, query)?;
        let records = match execution {
            ExecutionStep::IndexScan(index_scan) => {
                let starting_key = build_starting_key(schema, &index_scan)?;
                self.query_with_secondary_index(&starting_key, query.limit, query.skip)?
            }
            ExecutionStep::SeqScan(_seq_scan) => {
                self.iterate_and_deserialize(query.limit, query.skip)?
            }
        };

        Ok(records)
    }

    pub fn iterate_and_deserialize(
        &self,
        limit: usize,
        skip: usize,
    ) -> Result<Vec<Record>, CacheError> {
        let cursor = self
            .txn
            .open_ro_cursor(self.db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let mut cache_iterator = CacheIterator::new(&cursor, None, true);
        // cache_iterator.skip(skip);

        let mut records = vec![];
        let mut idx = 0;
        loop {
            let rec = cache_iterator.next();
            if skip > idx {
                //
            } else if rec.is_some() && idx < limit {
                if let Some((_key, val)) = rec {
                    let rec = bincode::deserialize::<Record>(val).map_err(|e| {
                        TypeError::SerializationError(
                            dozer_types::errors::types::SerializationError::Bincode(e),
                        )
                    })?;
                    records.push(rec);
                } else {
                    break;
                }
            } else {
                break;
            }
            idx += 1;
        }
        Ok(records)
    }

    fn query_with_secondary_index(
        &self,
        starting_key: &[u8],
        limit: usize,
        skip: usize,
    ) -> Result<Vec<Record>, CacheError> {
        let cursor = self
            .txn
            .open_ro_cursor(self.indexer_db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        let mut cache_iterator = CacheIterator::new(&cursor, Some(starting_key), true);
        let mut pkeys = vec![];
        let mut idx = 0;
        loop {
            if skip > idx {
            } else if idx < limit {
                let tuple = cache_iterator.next();

                // Check if the tuple returns a value
                if let Some((key, val)) = tuple {
                    // Compare partial key
                    if self.compare_key(key, starting_key) {
                        let rec = helper::get(self.txn, self.db, val)?;
                        pkeys.push(rec);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
            idx += 1;
        }
        Ok(pkeys)
    }

    fn compare_key(&self, key: &[u8], starting_key: &[u8]) -> bool {
        // TODO: find a better implementation
        // Find for partial matches if iterating on a query
        matches!(gs_find(key, starting_key), Some(_idx))
    }
}

fn build_starting_key(schema: &Schema, index_scan: &IndexScan) -> Result<Vec<u8>, CacheError> {
    let schema_identifier = schema
        .identifier
        .clone()
        .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;

    let mut fields = vec![];

    for (idx, idf) in index_scan.fields.iter().enumerate() {
        // Convert dynamic json_values to field_values based on field_types
        fields.push(match idf {
            Some(val) => {
                let field_type = schema
                    .fields
                    .get(idx)
                    .map_or(Err(CacheError::QueryError(QueryError::GetValue)), Ok)?
                    .typ
                    .to_owned();
                Some(
                    json_value_to_field(&val.to_string(), &field_type)
                        .map_err(CacheError::TypeError)?,
                )
            }
            None => None,
        });
    }

    match index_scan.index_def.typ {
        dozer_types::types::IndexType::SortedInverted => {
            let mut field_bytes = vec![];
            for field in fields {
                // convert value to `Vec<u8>`
                field_bytes.push(match field {
                    Some(field) => Some(
                        bincode::serialize(&field).map_err(CacheError::map_serialization_error)?,
                    ),
                    None => None,
                })
            }

            Ok(index::get_secondary_index(
                schema_identifier.id,
                &index_scan.index_def.fields,
                &field_bytes,
            ))
        }
        dozer_types::types::IndexType::HashInverted => todo!(),
        dozer_types::types::IndexType::FullText => {
            if fields.len() != 1 {
                return Err(CacheError::IndexError(IndexError::ExpectedStringFullText));
            }
            let field_index = index_scan.index_def.fields[0] as u64;

            if let Some(Field::String(token)) = &fields[0] {
                Ok(index::get_full_text_secondary_index(
                    schema_identifier.id,
                    field_index,
                    token,
                ))
            } else {
                Err(CacheError::IndexError(IndexError::ExpectedStringFullText))
            }
        }
    }
}
