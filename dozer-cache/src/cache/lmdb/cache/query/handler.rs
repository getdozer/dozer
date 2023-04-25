use super::intersection::intersection;
use crate::cache::expression::Skip;
use crate::cache::lmdb::cache::main_environment::MainEnvironment;
use crate::cache::lmdb::cache::query::secondary::build_index_scan;
use crate::cache::lmdb::cache::LmdbCache;
use crate::cache::CacheRecord;
use crate::cache::{
    expression::QueryExpression,
    plan::{IndexScan, Plan, QueryPlanner},
};
use crate::errors::{CacheError, PlanError};
use dozer_storage::errors::StorageError;
use dozer_storage::lmdb::{RoTransaction, Transaction};
use dozer_storage::LmdbEnvironment;
use dozer_types::borrow::IntoOwned;
use itertools::Either;

pub struct LmdbQueryHandler<'a, C: LmdbCache> {
    cache: &'a C,
    query: &'a QueryExpression,
}

impl<'a, C: LmdbCache> LmdbQueryHandler<'a, C> {
    pub fn new(cache: &'a C, query: &'a QueryExpression) -> Self {
        Self { cache, query }
    }

    pub fn count(&self) -> Result<usize, CacheError> {
        match self.plan()? {
            Plan::IndexScans(index_scans) => {
                let secondary_txns = self.create_secondary_txns(&index_scans)?;
                let ids = self.combine_secondary_queries(&index_scans, &secondary_txns)?;
                self.count_secondary_queries(ids)
            }
            Plan::SeqScan(_) => Ok(match self.query.skip {
                Skip::Skip(skip) => self
                    .cache
                    .main_env()
                    .count()?
                    .saturating_sub(skip)
                    .min(self.query.limit.unwrap_or(usize::MAX)),
                Skip::After(_) => self.all_ids(&self.cache.main_env().begin_txn()?)?.count(),
            }),
            Plan::ReturnEmpty => Ok(0),
        }
    }

    pub fn query(&self) -> Result<Vec<CacheRecord>, CacheError> {
        match self.plan()? {
            Plan::IndexScans(index_scans) => {
                let secondary_txns = self.create_secondary_txns(&index_scans)?;
                let main_txn = self.cache.main_env().begin_txn()?;
                #[allow(clippy::let_and_return)] // Must do let binding unless won't compile
                let result = self.collect_records(
                    &main_txn,
                    self.combine_secondary_queries(&index_scans, &secondary_txns)?,
                );
                result
            }
            Plan::SeqScan(_seq_scan) => {
                let main_txn = self.cache.main_env().begin_txn()?;
                #[allow(clippy::let_and_return)] // Must do let binding unless won't compile
                let result = self.collect_records(&main_txn, self.all_ids(&main_txn)?);
                result
            }
            Plan::ReturnEmpty => Ok(vec![]),
        }
    }

    fn plan(&self) -> Result<Plan, PlanError> {
        let (schema, secondary_indexes) = self.cache.main_env().schema();
        let planner = QueryPlanner::new(
            schema,
            secondary_indexes,
            self.query.filter.as_ref(),
            &self.query.order_by,
        );
        planner.plan()
    }

    fn all_ids<'txn, T: Transaction>(
        &self,
        main_txn: &'txn T,
    ) -> Result<impl Iterator<Item = Result<u64, CacheError>> + 'txn, CacheError> {
        let schema_is_append_only = self.cache.main_env().schema().0.is_append_only();
        let all_ids = self
            .cache
            .main_env()
            .operation_log()
            .present_operation_ids(main_txn, schema_is_append_only)?
            .map(|result| {
                result
                    .map(|id| id.into_owned())
                    .map_err(CacheError::Storage)
            });
        Ok(skip(all_ids, self.query.skip).take(self.query.limit.unwrap_or(usize::MAX)))
    }

    fn create_secondary_txns(
        &self,
        index_scans: &[IndexScan],
    ) -> Result<Vec<RoTransaction<'_>>, StorageError> {
        index_scans
            .iter()
            .map(|index_scan| self.cache.secondary_env(index_scan.index_id).begin_txn())
            .collect()
    }

    fn combine_secondary_queries<'txn, T: Transaction>(
        &self,
        index_scans: &[IndexScan],
        secondary_txns: &'txn [T],
    ) -> Result<impl Iterator<Item = Result<u64, CacheError>> + 'txn, CacheError> {
        debug_assert!(
            !index_scans.is_empty(),
            "Planner should not generate empty index scan"
        );
        let combined = if index_scans.len() == 1 {
            // The fast path, without intersection calculation.
            Either::Left(build_index_scan(
                &secondary_txns[0],
                self.cache.secondary_env(index_scans[0].index_id),
                &index_scans[0].kind,
            )?)
        } else {
            // Intersection of multiple index scans.
            let iterators = index_scans
                .iter()
                .zip(secondary_txns)
                .map(|(index_scan, secondary_txn)| {
                    build_index_scan(
                        secondary_txn,
                        self.cache.secondary_env(index_scan.index_id),
                        &index_scan.kind,
                    )
                })
                .collect::<Result<Vec<_>, CacheError>>()?;
            Either::Right(intersection(
                iterators,
                self.cache.main_env().intersection_chunk_size(),
            ))
        };
        Ok(skip(combined, self.query.skip).take(self.query.limit.unwrap_or(usize::MAX)))
    }

    fn filter_secondary_queries<'txn, T: Transaction>(
        &'txn self,
        main_txn: &'txn T,
        ids: impl Iterator<Item = Result<u64, CacheError>> + 'txn,
    ) -> impl Iterator<Item = Result<u64, CacheError>> + 'txn {
        let schema_is_append_only = self.cache.main_env().schema().0.is_append_only();
        ids.filter_map(move |id| match id {
            Ok(id) => match self.cache.main_env().operation_log().contains_operation_id(
                main_txn,
                schema_is_append_only,
                id,
            ) {
                Ok(true) => Some(Ok(id)),
                Ok(false) => None,
                Err(err) => Some(Err(err.into())),
            },
            Err(err) => Some(Err(err)),
        })
    }

    fn count_secondary_queries(
        &self,
        ids: impl Iterator<Item = Result<u64, CacheError>>,
    ) -> Result<usize, CacheError> {
        let main_txn = self.cache.main_env().begin_txn()?;

        let mut result = 0;
        for maybe_id in self.filter_secondary_queries(&main_txn, ids) {
            maybe_id?;
            result += 1;
        }
        Ok(result)
    }

    fn collect_records<'txn, T: Transaction>(
        &'txn self,
        main_txn: &'txn T,
        ids: impl Iterator<Item = Result<u64, CacheError>> + 'txn,
    ) -> Result<Vec<CacheRecord>, CacheError> {
        self.filter_secondary_queries(main_txn, ids)
            .map(|id| {
                id.and_then(|id| {
                    self.cache
                        .main_env()
                        .operation_log()
                        .get_record_by_operation_id_unchecked(main_txn, id)
                        .map_err(Into::into)
                })
            })
            .collect()
    }
}

fn skip(
    iter: impl Iterator<Item = Result<u64, CacheError>>,
    skip: Skip,
) -> impl Iterator<Item = Result<u64, CacheError>> {
    match skip {
        Skip::Skip(n) => Either::Left(iter.skip(n)),
        Skip::After(after) => Either::Right(skip_after(iter, after)),
    }
}

struct SkipAfter<T: Iterator<Item = Result<u64, CacheError>>> {
    inner: T,
    after: Option<u64>,
}

impl<T: Iterator<Item = Result<u64, CacheError>>> Iterator for SkipAfter<T> {
    type Item = Result<u64, CacheError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(after) = self.after {
                match self.inner.next() {
                    Some(Ok(id)) => {
                        if id == after {
                            self.after = None;
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                }
            } else {
                return self.inner.next();
            }
        }
    }
}

fn skip_after<T: Iterator<Item = Result<u64, CacheError>>>(iter: T, after: u64) -> SkipAfter<T> {
    SkipAfter {
        inner: iter,
        after: Some(after),
    }
}
