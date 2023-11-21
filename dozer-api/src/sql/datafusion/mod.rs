pub mod json;
mod predicate_pushdown;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::information_schema::InformationSchemaProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::config::{ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::datasource::{DefaultTableSource, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_expr::var_provider::is_system_variables;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::{ContextProvider, ParserOptions, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::{ResolvedTableReference, TableReference};
use datafusion::variable::{VarProvider, VarType};
use datafusion_expr::{
    AggregateUDF, Expr, LogicalPlan, ScalarUDF, TableProviderFilterPushDown, TableSource, WindowUDF,
};
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
    pub ctx: Arc<SessionContext>,
}

struct ContextResolver {
    tables: HashMap<String, Arc<dyn TableSource>>,
    state: SessionState,
}

impl ContextProvider for ContextResolver {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn datafusion_expr::TableSource>> {
        if let Some(table) = PgCatalogTable::from_ref(&name) {
            Ok(Arc::new(DefaultTableSource::new(Arc::new(table))))
        } else {
            let catalog = &self.state.config_options().catalog;
            let name = name
                .resolve(&catalog.default_catalog, &catalog.default_schema)
                .to_string();
            self.tables
                .get(&name)
                .ok_or_else(|| DataFusionError::Plan(format!("table '{name}' not found")))
                .cloned()
        }
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        let mut parts = name.splitn(3, '.');
        let first = parts.next()?;
        let second = parts.next();
        let third = parts.next();
        match (first, second, third) {
            (_, Some("pg_catalog"), Some(name))
            | ("pg_catalog", Some(name), None)
            | (name, None, None) => match name {
                "version" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.version".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(|_| {
                            Ok(datafusion_expr::ColumnarValue::Scalar(
                                datafusion::scalar::ScalarValue::Utf8(Some(format!(
                                    "PostgreSQL 9.0 (Dozer {})",
                                    env!("CARGO_PKG_VERSION")
                                ))),
                            ))
                        }),
                    }))
                }
                "current_schema" => {
                    let schema = self.state.config_options().catalog.default_schema.clone();
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.current_schema".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(move |_| {
                            Ok(datafusion_expr::ColumnarValue::Scalar(
                                datafusion::scalar::ScalarValue::Utf8(Some(schema.clone())),
                            ))
                        }),
                    }));
                }
                _ => {}
            },
            _ => {}
        }

        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.state
            .execution_props()
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
}

impl SQLExecutor {
    pub fn new(cache_endpoints: Vec<Arc<CacheEndpoint>>) -> Self {
        let ctx = SessionContext::new_with_config(
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

        let variable_provider = Arc::new(SystemVariables);
        ctx.register_variable(VarType::UserDefined, variable_provider.clone());
        ctx.register_variable(VarType::System, variable_provider);

        Self { ctx: Arc::new(ctx) }
    }

    pub async fn execute(&self, plan: LogicalPlan) -> Result<DataFrame, DataFusionError> {
        self.ctx.execute_logical_plan(plan).await
    }

    fn schema_for_ref(
        &self,
        state: &SessionState,
        resolved_ref: ResolvedTableReference<'_>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        if state.config().information_schema() && resolved_ref.schema == "information_schema" {
            return Ok(Arc::new(InformationSchemaProvider::new(
                state.catalog_list().clone(),
            )));
        }

        state
            .catalog_list()
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                ))
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("failed to resolve schema: {}", resolved_ref.schema))
            })
    }

    pub async fn parse(&self, sql: &str) -> Result<Option<LogicalPlan>, DataFusionError> {
        println!("@@ query: {sql}");
        let mut statement = self.ctx.state().sql_to_statement(sql, "postgres")?;
        let rewrite = if let datafusion::sql::parser::Statement::Statement(ref stmt) = statement {
            match stmt.as_ref() {
                ast::Statement::StartTransaction { .. }
                | ast::Statement::Commit { .. }
                | ast::Statement::Rollback { .. } => {
                    return Ok(None);
                }
                ast::Statement::ShowVariable { variable } => {
                    let variable = object_name_to_string(&variable);
                    match variable.as_str() {
                        "transaction.isolation.level" => Some("SELECT \"@@transaction_isolation\""),
                        "standard_conforming_strings" => {
                            Some("SELECT \"@@standard_conforming_strings\"")
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        };
        if let Some(query) = rewrite {
            statement = self.ctx.state().sql_to_statement(query, "postgres")?;
        }
        let state = self.ctx.state();
        let table_refs = state.resolve_table_references(&statement)?;

        let mut provider = ContextResolver {
            state,
            tables: HashMap::with_capacity(table_refs.len()),
        };
        let state = self.ctx.state();
        let config = state.config_options();
        let default_catalog = &config.catalog.default_catalog;
        let default_schema = &config.catalog.default_schema;
        for table_ref in table_refs {
            let table = table_ref.table();
            let resolved = table_ref.clone().resolve(default_catalog, default_schema);
            if let Entry::Vacant(v) = provider.tables.entry(resolved.to_string()) {
                if let Ok(schema) = self.schema_for_ref(&state, resolved) {
                    if let Some(table) = schema.table(table).await {
                        v.insert(Arc::new(DefaultTableSource::new(table)));
                    }
                }
            }
        }

        let query = SqlToRel::new_with_options(
            &provider,
            ParserOptions {
                parse_float_as_decimal: false,
                enable_ident_normalization: true,
            },
        );
        Some(query.statement_to_plan(statement)).transpose()
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
pub struct PgCatalogTable {
    schema: SchemaRef,
}

macro_rules! schema {
    ({$($name:literal: $type:path[$nullable:literal]),*})  => {{
        let v = vec![$(Field::new($name, $type, $nullable)),*];

        Arc::new(Schema::new(v))
    }};
}

impl PgCatalogTable {
    pub fn from_ref(reference: &TableReference) -> Option<Self> {
        match reference.schema() {
            Some("pg_catalog") | None => (),
            _ => return None,
        }
        match reference.table() {
            "pg_type" => Some(Self::pg_type()),
            "pg_namespace" => Some(Self::pg_namespace()),
            "pg_proc" => Some(Self::pg_proc()),
            "pg_class" => Some(Self::pg_class()),
            "pg_attribute" => Some(Self::pg_attribute()),
            _ => None,
        }
    }

    pub fn pg_type() -> Self {
        Self {
            schema: schema!({
                "oid"               : DataType::Utf8[false],
                "typname"           : DataType::Utf8[false],
                "typnamespace"      : DataType::Utf8[false],
                "typowner"          : DataType::Utf8[false],
                "typlen"            : DataType::Int16[false],
                "typbyval"          : DataType::Boolean[false],
                "typtype"           : DataType::Utf8[false],
                "typcategory"       : DataType::Utf8[false],
                "typispreferred"    : DataType::Boolean[false],
                "typisdefined"      : DataType::Boolean[false],
                "typdelim"          : DataType::Utf8[false],
                "typrelid"          : DataType::Utf8[false],
                "typelem"           : DataType::Utf8[false],
                "typarray"          : DataType::Utf8[false],
                "typinput"          : DataType::Utf8[false],
                "typoutput"         : DataType::Utf8[false],
                "typreceive"        : DataType::Utf8[false],
                "typsend"           : DataType::Utf8[false],
                "typmodin"          : DataType::Utf8[false],
                "typmodout"         : DataType::Utf8[false],
                "typanalyze"        : DataType::Utf8[false],
                "typalign"          : DataType::Utf8[false],
                "typstorage"        : DataType::Utf8[false],
                "typnotnull"        : DataType::Boolean[false],
                "typbasetype"       : DataType::Utf8[false],
                "typtypmod"         : DataType::Int32[false],
                "typndims"          : DataType::Int32[false],
                "typcollation"      : DataType::Utf8[false],
                "typdefaultbin"     : DataType::Binary[true],
                "typdefault"        : DataType::Utf8[true],
                "typacl"            : DataType::Utf8[true]
            }),
        }
    }
    pub fn pg_namespace() -> Self {
        Self {
            schema: schema!({
                "oid"       : DataType::UInt32[false],
                "nspname"   : DataType::Utf8[false],
                "nspowner"  : DataType::Utf8[false],
                "nspacl"    : DataType::Utf8[false]
            }),
        }
    }

    pub fn pg_proc() -> Self {
        Self {
            schema: schema!({
                 "oid"             : DataType::UInt32[false],
                 "proname"         : DataType::Utf8[false],
                 "pronamespace"    : DataType::UInt32[false],
                 "proowner"        : DataType::UInt32[false],
                 "prolang"         : DataType::UInt32[false],
                 "procost"         : DataType::Float64[false],
                 "prorows"         : DataType::Float64[false],
                 "provariadic"     : DataType::UInt32[false],
                 "prosupport"      : DataType::UInt32[false],
                 "prokind"         : DataType::Utf8[false],
                 "prosecdef"       : DataType::Boolean[false],
                 "proleakproof"    : DataType::Boolean[false],
                 "proisstrict"     : DataType::Boolean[false],
                 "proretset"       : DataType::Boolean[false],
                 "provolatile"     : DataType::Utf8[false],
                 "proparallel"     : DataType::Utf8[false],
                 "pronargs"        : DataType::Int16[false],
                 "pronargdefaults" : DataType::Int16[false],
                 "prorettype"      : DataType::UInt32[false],
                 "proargtypes"     : DataType::Utf8[false],
                 "proallargtypes"  : DataType::Utf8[true],
                 "proargmodes"     : DataType::Utf8[true],
                 "proargnames"     : DataType::Utf8[true],
                 "proargdefaults"  : DataType::Utf8[true],
                 "protrftypes"     : DataType::Utf8[true],
                 "prosrc"          : DataType::Utf8[false],
                 "probin"          : DataType::Utf8[true],
                 "prosqlbody"      : DataType::Utf8[true],
                 "proconfig"       : DataType::Utf8[true],
                 "proacl"          : DataType::Utf8[true]
            }),
        }
    }

    fn pg_attribute() -> Self {
        Self {
            schema: schema!({
                "attrelid"       : DataType::UInt32[false],
                "attname"        : DataType::Utf8[false],
                "atttypid"       : DataType::UInt32[false],
                "attlen"         : DataType::Int16[false],
                "attnum"         : DataType::Int16[false],
                "attcacheoff"    : DataType::Int32[false],
                "atttypmod"      : DataType::Int32[false],
                "attndims"       : DataType::Int16[false],
                "attbyval"       : DataType::Boolean[false],
                "attalign"       : DataType::Utf8[false],
                "attstorage"     : DataType::Utf8[false],
                "attcompression" : DataType::Utf8[false],
                "attnotnull"     : DataType::Boolean[false],
                "atthasdef"      : DataType::Boolean[false],
                "atthasmissing"  : DataType::Boolean[false],
                "attidentity"    : DataType::Utf8[false],
                "attgenerated"   : DataType::Utf8[false],
                "attisdropped"   : DataType::Boolean[false],
                "attislocal"     : DataType::Boolean[false],
                "attinhcount"    : DataType::UInt16[false],
                "attstattarget"  : DataType::UInt16[false],
                "attcollation"   : DataType::UInt32[false],
                "attacl"         : DataType::Utf8[true],
                "attoptions"     : DataType::Utf8[true],
                "attfdwoptions"  : DataType::Utf8[true],
                "attmissingval"  : DataType::Utf8[true]
            }),
        }
    }

    fn pg_class() -> Self {
        Self {
            schema: schema! ({
                 "oid"                : DataType::UInt32[false],
                 "relname"             : DataType::Utf8[false],
                 "relnamespace"        : DataType::UInt32[false],
                 "reltype"             : DataType::UInt32[false],
                 "reloftype"           : DataType::UInt32[false],
                 "relowner"            : DataType::UInt32[false],
                 "relam"               : DataType::UInt32[false],
                 "relfilenode"         : DataType::UInt32[false],
                 "reltablespace"       : DataType::UInt32[false],
                 "relpages"            : DataType::Int32[false],
                 "reltuples"           : DataType::Float64[false],
                 "relallvisible"       : DataType::Int32[false],
                 "reltoastrelid"       : DataType::UInt32[false],
                 "relhasindex"         : DataType::Boolean[false],
                 "relisshared"         : DataType::Boolean[false],
                 "relpersistence"      : DataType::Utf8[false],
                 "relkind"             : DataType::Utf8[false],
                 "relnatts"            : DataType::Int16[false],
                 "relchecks"           : DataType::Int16[false],
                 "relhasrules"         : DataType::Boolean[false],
                 "relhastriggers"      : DataType::Boolean[false],
                 "relhassubclass"      : DataType::Boolean[false],
                 "relrowsecurity"      : DataType::Boolean[false],
                 "relforcerowsecurity" : DataType::Boolean[false],
                 "relispopulated"      : DataType::Boolean[false],
                 "relreplident"        : DataType::Utf8[false],
                 "relispartition"      : DataType::Boolean[false],
                 "relrewrite"          : DataType::UInt32[false],
                 "relfrozenxid"        : DataType::UInt32[false],
                 "relminmxid"          : DataType::UInt32[false],
                 "relacl"              : DataType::Utf8[true],
                 "reloptions"          : DataType::Utf8[true],
                 "relpartbound"        : DataType::Utf8[true]
            }),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogTable {
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

#[derive(Debug)]
struct SystemVariables;

impl VarProvider for SystemVariables {
    fn get_value(&self, var_names: Vec<String>) -> Result<datafusion::scalar::ScalarValue> {
        if var_names.len() == 1 {
            match var_names[0].as_str() {
                "@@transaction_isolation" => {
                    return Ok(ScalarValue::Utf8(Some("read committed".into())))
                }
                "@@standard_conforming_strings" => return Ok(ScalarValue::Utf8(Some("on".into()))),
                _ => (),
            }
        }
        Err(DataFusionError::Internal(format!(
            "unrecognized variable {var_names:?}"
        )))
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        if var_names.len() == 1 {
            match var_names[0].as_str() {
                "@@transaction_isolation" | "@@standard_conforming_strings" => {
                    return Some(DataType::Utf8)
                }
                _ => (),
            }
        }
        None
    }
}

fn object_name_to_string(object_name: &[ast::Ident]) -> String {
    object_name
        .iter()
        .map(ident_to_string)
        .collect::<Vec<String>>()
        .join(".")
}

fn ident_to_string(ident: &ast::Ident) -> String {
    normalize_ident(ident.to_owned())
}

fn normalize_ident(id: ast::Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}
