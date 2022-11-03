use std::{ffi::c_void, sync::Arc};

use super::{helper, iterator::CacheIterator};
use crate::cache::{
    expression::{Operator, QueryExpression},
    index::{self},
    lmdb::cache::IndexMetaData,
    plan::{IndexFilter, IndexScan, Plan, QueryPlanner},
};
use dozer_types::{
    bincode, json_value_to_field,
    types::{Field, Record, Schema, SortDirection},
};
use dozer_types::{
    errors::{
        cache::{
            CacheError::{self},
            IndexError, QueryError,
        },
        types::TypeError,
    },
    types::IndexDefinition,
};
use lmdb::{Database, RoTransaction, Transaction};
use lmdb_sys as ffi;
pub struct LmdbQueryHandler<'a> {
    db: Database,
    index_metadata: Arc<IndexMetaData>,
    txn: &'a RoTransaction<'a>,
    schema: &'a Schema,
    query: &'a QueryExpression,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(
        db: Database,
        index_metadata: Arc<IndexMetaData>,
        txn: &'a RoTransaction,
        schema: &'a Schema,
        query: &'a QueryExpression,
    ) -> Self {
        Self {
            db,
            index_metadata,
            txn,
            schema,
            query,
        }
    }

    pub fn query(&self) -> Result<Vec<Record>, CacheError> {
        let planner = QueryPlanner::new(self.schema, self.query);
        let execution = planner.plan()?;
        let records = match execution {
            Plan::IndexScans(index_scans) => {
                if index_scans.len() > 1 {
                    todo!("Combine results from multiple index scans");
                }
                debug_assert!(
                    !index_scans.is_empty(),
                    "Planner should not generate empty index scan"
                );
                self.query_with_secondary_index(&index_scans)?
            }
            Plan::SeqScan(_seq_scan) => self.iterate_and_deserialize()?,
        };

        Ok(records)
    }

    pub fn iterate_and_deserialize(&self) -> Result<Vec<Record>, CacheError> {
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
            if self.query.skip > idx {
                //
            } else if rec.is_some() && idx < self.query.limit {
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
        index_scans: &[IndexScan],
    ) -> Result<Vec<Record>, CacheError> {
        // TODO: Use the opposite sort on reversed queries.
        let sort_order = true;
        let index_scan = index_scans[0].to_owned();
        let db = self
            .index_metadata
            .get_db(self.schema, index_scan.index_id.unwrap());

        let comparision_key = self.build_comparision_key(&index_scan)?;
        let last_filter = index_scan.filters.last().unwrap().to_owned();

        let (start_key, end_key) =
            get_start_end_keys(last_filter.as_ref(), sort_order, comparision_key);

        let mut pkeys = vec![];
        let mut idx = 0;

        let cursor = self
            .txn
            .open_ro_cursor(db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        let mut cache_iterator =
            CacheIterator::new(&cursor, start_key.as_ref().map(|a| a as &[u8]), sort_order);
        // For GT, LT operators dont include the first record returned.

        loop {
            let tuple = cache_iterator.next();

            if self.query.skip > idx {
            } else if idx < self.query.limit {
                // Check if the tuple returns a value
                if let Some((key, val)) = tuple {
                    // Skip Eq Values
                    if self.skip_eq_values(
                        last_filter.as_ref(),
                        start_key.as_ref(),
                        end_key.as_ref(),
                        key,
                        sort_order,
                    ) {
                        println!("skipping....");
                    }
                    // Compare partial key
                    else if self.compare_key(
                        key,
                        start_key.as_ref(),
                        end_key.as_ref(),
                        sort_order,
                        last_filter.as_ref(),
                    ) {
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

    // Based on the filters provided and sort_order, determine to include the first result.
    fn skip_eq_values(
        &self,
        last_filter: Option<&IndexFilter>,
        start_key: Option<&Vec<u8>>,
        end_key: Option<&Vec<u8>>,
        current_key: &[u8],
        sort_order: bool,
    ) -> bool {
        let val = last_filter.map_or(false, |f| match f.op {
            Operator::LT => {
                if sort_order {
                    false
                } else {
                    let end_cmp = self.lmdb_cmp(current_key, end_key);
                    end_cmp == 0
                }
            }
            Operator::GT => {
                if sort_order {
                    let cmp = self.lmdb_cmp(current_key, start_key);
                    cmp == 0
                } else {
                    false
                }
            }
            Operator::GTE
            | Operator::LTE
            | Operator::EQ
            | Operator::Contains
            | Operator::MatchesAny
            | Operator::MatchesAll => false,
        });

        println!("skip_eq_values: {:?}", val);
        val
    }

    fn lmdb_cmp(&self, a: &[u8], b: Option<&Vec<u8>>) -> i32 {
        if let Some(b) = b {
            let key_val: ffi::MDB_val = ffi::MDB_val {
                mv_size: a.len(),
                mv_data: a.as_ptr() as *mut c_void,
            };
            let start_key_val: ffi::MDB_val = ffi::MDB_val {
                mv_size: b.len(),
                mv_data: b.as_ptr() as *mut c_void,
            };
            unsafe { lmdb_sys::mdb_cmp(self.txn.txn(), self.db.dbi(), &key_val, &start_key_val) }
        } else {
            2
        }
    }

    fn compare_key(
        &self,
        key: &[u8],
        start_key: Option<&Vec<u8>>,
        end_key: Option<&Vec<u8>>,
        sort_order: bool,
        last_filter: Option<&IndexFilter>,
    ) -> bool {
        let cmp = self.lmdb_cmp(key, start_key);
        let end_cmp = self.lmdb_cmp(key, end_key);

        println!("Cmp: {:?}, End Cmp: {:?}", cmp, end_cmp);

        last_filter.map_or(cmp == 0, |f| {
            let valid = match f.op {
                Operator::LT => {
                    if sort_order {
                        end_cmp < 0
                    } else {
                        cmp >= 0
                    }
                }
                Operator::LTE => {
                    if sort_order {
                        end_cmp <= 0
                    } else {
                        cmp > 0
                    }
                }

                Operator::GT => {
                    if sort_order {
                        cmp > 0
                    } else {
                        end_cmp <= 0
                    }
                }
                Operator::GTE => {
                    if sort_order {
                        cmp >= 0
                    } else {
                        end_cmp < 0
                    }
                }
                Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
                    cmp == 0
                }
            };
            println!("--valid: {:?}, operator: {:?}", valid, f.op);
            valid
        })
    }

    fn build_comparision_key(&self, index_scan: &'a IndexScan) -> Result<Vec<u8>, CacheError> {
        let schema_identifier = self
            .schema
            .identifier
            .clone()
            .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;

        let mut fields = vec![];

        for (idx, idf) in index_scan.filters.iter().enumerate() {
            // Convert dynamic json_values to field_values based on field_types
            fields.push(match idf {
                Some(idf) => {
                    let field_type = self
                        .schema
                        .fields
                        .get(idx)
                        .map_or(Err(CacheError::QueryError(QueryError::GetValue)), Ok)?
                        .typ
                        .to_owned();
                    Some(
                        json_value_to_field(&idf.val.to_string(), &field_type)
                            .map_err(CacheError::TypeError)?,
                    )
                }
                None => None,
            });
        }

        match &index_scan.index_def {
            IndexDefinition::SortedInverted(field_indices) => {
                Ok(self.build_composite_range_key(field_indices, fields)?)
            }
            IndexDefinition::FullText(field_index) => {
                if let Some(Field::String(token)) = &fields[0] {
                    Ok(index::get_full_text_secondary_index(
                        schema_identifier.id,
                        *field_index as _,
                        token,
                    ))
                } else {
                    Err(CacheError::IndexError(IndexError::ExpectedStringFullText))
                }
            }
        }
    }

    fn build_composite_range_key(
        &self,
        field_indices: &[(usize, SortDirection)],
        fields: Vec<Option<Field>>,
    ) -> Result<Vec<u8>, CacheError> {
        let schema_identifier = self
            .schema
            .identifier
            .clone()
            .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;

        let field_indices: Vec<_> = field_indices.iter().map(|(index, _)| *index).collect();
        let mut field_bytes = vec![];
        for field in fields {
            // convert value to `Vec<u8>`
            field_bytes.push(match field {
                Some(field) => Some(field.to_bytes().map_err(CacheError::TypeError)?),
                None => None,
            })
        }

        Ok(index::get_secondary_index(
            schema_identifier.id,
            &field_indices,
            &field_bytes,
        ))
    }
}

fn get_start_end_keys(
    last_filter: Option<&IndexFilter>,
    sort_order: bool,
    comp_key: Vec<u8>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    last_filter.map_or((Some(comp_key.to_owned()), None), |f| match f.op {
        Operator::LT | Operator::LTE => {
            if sort_order {
                (None, Some(comp_key))
            } else {
                (Some(comp_key), None)
            }
        }

        Operator::GT | Operator::GTE => {
            if sort_order {
                (Some(comp_key), None)
            } else {
                (None, Some(comp_key))
            }
        }
        Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
            (Some(comp_key), None)
        }
    })
}
