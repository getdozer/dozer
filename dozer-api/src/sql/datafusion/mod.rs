mod predicate_pushdown;

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use dozer_types::arrow::datatypes::SchemaRef;
use dozer_types::arrow::record_batch::RecordBatch;

use dozer_cache::cache::{expression::QueryExpression, CacheRecord};
use dozer_types::arrow_types::to_arrow::{map_record_to_arrow, map_to_arrow_schema};
use dozer_types::log::debug;
use dozer_types::types::Schema as DozerSchema;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

use crate::api_helper::get_records;
use crate::CacheEndpoint;

use predicate_pushdown::{predicate_pushdown, supports_predicates_pushdown};

pub struct SQLExecutor {
    ctx: SessionContext,
}

impl SQLExecutor {
    pub fn new(cache_endpoints: Vec<Arc<CacheEndpoint>>) -> Self {
        let ctx = SessionContext::with_config(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema("public", "dozer"),
        );
        for cache_endpoint in cache_endpoints {
            let data_source = CacheEndpointDataSource::new(cache_endpoint.clone());
            let _provider = ctx
                .register_table(
                    TableReference::Bare {
                        table: cache_endpoint.endpoint().name.clone().into(),
                    },
                    Arc::new(data_source),
                )
                .unwrap();
        }
        {
            let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
            let pg_type = Arc::new(PgTypesView::new());
            schema.register_table("pg_type".into(), pg_type).unwrap();
            ctx.catalog("public")
                .unwrap()
                .register_schema("pg_catalog", schema.clone())
                .unwrap();
        }
        Self { ctx }
    }

    pub async fn execute(&self, sql: &str) -> Result<DataFrame, DataFusionError> {
        self.ctx.sql(sql).await
    }
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Debug, Clone)]
pub struct CacheEndpointDataSource {
    cache_endpoint: Arc<CacheEndpoint>,
    schema: SchemaRef,
}

impl CacheEndpointDataSource {
    pub fn new(cache_endpoint: Arc<CacheEndpoint>) -> Self {
        let schema = {
            let cache_reader = &cache_endpoint.cache_reader();
            let schema = &cache_reader.get_schema().0;
            Arc::new(map_to_arrow_schema(schema).unwrap())
        };
        Self {
            cache_endpoint,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for CacheEndpointDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CacheEndpointExec::try_new(
            self.cache_endpoint.clone(),
            self.schema.clone(),
            projection,
            filters.to_vec(),
            limit,
        )?))
    }

    // fn supports_filter_pushdown(&self, filter: &Expr) -> Result<TableProviderFilterPushDown> {
    //     supports_predicate_pushdown(filter)
    // }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(supports_predicates_pushdown(
            filters,
            self.cache_endpoint.clone(),
        ))
    }
}

#[derive(Debug)]
pub struct CacheEndpointExec {
    cache_endpoint: Arc<CacheEndpoint>,
    projection: Option<Arc<[usize]>>,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl CacheEndpointExec {
    /// Try to create a new [`StreamingTableExec`] returning an error if the schema is incorrect
    pub fn try_new(
        cache_endpoint: Arc<CacheEndpoint>,
        schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = match projection {
            Some(p) => Arc::new(schema.project(p)?),
            None => schema,
        };

        Ok(Self {
            cache_endpoint,
            projected_schema,
            projection: projection.cloned().map(Into::into),
            filters,
            limit,
        })
    }
}

#[async_trait]
impl ExecutionPlan for CacheEndpointExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(false)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("Children cannot be replaced in {self:?}")
    }

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = futures_util::stream::iter({
            let cache_reader = &self.cache_endpoint.cache_reader();
            let mut expr = QueryExpression {
                limit: self.limit,
                filter: predicate_pushdown(self.filters.iter()),
                ..Default::default()
            };
            debug!("Using predicate pushdown {:?}", expr.filter);
            let records = get_records(
                cache_reader,
                &mut expr,
                &self.cache_endpoint.endpoint.name,
                None,
            )
            .unwrap();

            transpose(cache_reader.get_schema().0.clone(), records)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            match self.projection.clone() {
                Some(projection) => Box::pin(stream.map(move |x| {
                    x.and_then(|b| b.project(projection.as_ref()).map_err(Into::into))
                })) as BoxStream<'_, Result<RecordBatch>>,
                None => Box::pin(stream),
            },
        )))
    }

    fn statistics(&self) -> Statistics {
        Default::default()
    }
}

impl DisplayAs for CacheEndpointExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CacheEndpointExec",)
            }
        }
    }
}

fn transpose(
    schema: DozerSchema,
    records: Vec<CacheRecord>,
) -> impl Iterator<Item = Result<RecordBatch, DataFusionError>> {
    records.into_iter().map(move |CacheRecord { record, .. }| {
        map_record_to_arrow(record, &schema).map_err(DataFusionError::ArrowError)
    })
}

#[derive(Debug)]
pub struct PgTypesView {
    schema: SchemaRef,
}

impl Default for PgTypesView {
    fn default() -> Self {
        Self::new()
    }
}

impl PgTypesView {
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![
                Field::new("oid", DataType::Utf8, false),
                Field::new("typname", DataType::Utf8, false),
                Field::new("typnamespace", DataType::Utf8, false),
                Field::new("typowner", DataType::Utf8, false),
                Field::new("typlen", DataType::Int16, false),
                Field::new("typbyval", DataType::Boolean, false),
                Field::new("typtype", DataType::Utf8, false),
                Field::new("typcategory", DataType::Utf8, false),
                Field::new("typispreferred", DataType::Boolean, false),
                Field::new("typisdefined", DataType::Boolean, false),
                Field::new("typdelim", DataType::Utf8, false),
                Field::new("typrelid", DataType::Utf8, false),
                Field::new("typelem", DataType::Utf8, false),
                Field::new("typarray", DataType::Utf8, false),
                Field::new("typinput", DataType::Utf8, false),
                Field::new("typoutput", DataType::Utf8, false),
                Field::new("typreceive", DataType::Utf8, false),
                Field::new("typsend", DataType::Utf8, false),
                Field::new("typmodin", DataType::Utf8, false),
                Field::new("typmodout", DataType::Utf8, false),
                Field::new("typanalyze", DataType::Utf8, false),
                Field::new("typalign", DataType::Utf8, false),
                Field::new("typstorage", DataType::Utf8, false),
                Field::new("typnotnull", DataType::Boolean, false),
                Field::new("typbasetype", DataType::Utf8, false),
                Field::new("typtypmod", DataType::Int32, false),
                Field::new("typndims", DataType::Int32, false),
                Field::new("typcollation", DataType::Utf8, false),
                Field::new("typdefaultbin", DataType::Binary, true),
                Field::new("typdefault", DataType::Utf8, true),
                Field::new("typacl", DataType::Utf8, true),
            ])),
        }
    }
}

#[async_trait]
impl TableProvider for PgTypesView {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[],
            self.schema.clone(),
            projection.cloned(),
        )?))
    }
}
