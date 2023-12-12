pub mod json;
mod pg_catalog;
mod predicate_pushdown;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::information_schema::InformationSchemaProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::config::ConfigOptions;
use datafusion::datasource::streaming::StreamingTable;
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

use self::pg_catalog::PgCatalogTable;

pub struct SQLExecutor {
    pub ctx: Arc<SessionContext>,
}

struct ContextResolver {
    tables: HashMap<String, Arc<dyn TableSource>>,
    state: Arc<SessionState>,
}

impl ContextProvider for ContextResolver {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn datafusion_expr::TableSource>> {
        if let Some(table) = PgCatalogTable::from_ref_with_state(&name, self.state.clone()) {
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
                "format_type" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.format_type".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![
                                DataType::UInt32,
                                DataType::Int32,
                            ]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(move |_| {
                            Ok(datafusion_expr::ColumnarValue::Scalar(
                                datafusion::scalar::ScalarValue::Utf8(Some("".to_string())),
                            ))
                        }),
                    }));
                }
                "pg_get_expr" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_get_expr".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(move |_| {
                            Ok(datafusion_expr::ColumnarValue::Scalar(
                                datafusion::scalar::ScalarValue::Utf8(Some("".to_string())),
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
            return Ok(Arc::new(InformationSchemaProviderWrapper {
                inner: InformationSchemaProvider::new(state.catalog_list().clone()),
            }));
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

    pub async fn parse(&self, mut sql: &str) -> Result<Option<LogicalPlan>, DataFusionError> {
        println!("@@ query: {sql}");
        if sql
            .to_ascii_lowercase()
            .trim_start()
            .starts_with("select character_set_name")
        {
            sql = "select 'UTF8'"
        }
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
            state: Arc::new(state),
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

macro_rules! nullable_helper {
    (nullable) => {
        true
    };
    () => {
        false
    };
}

macro_rules! schema {
    ({$($name:literal: $type:path $(: $nullable:ident)?),* $(,)?})  => {{
        let v = vec![$(Field::new($name, $type, nullable_helper!($($nullable)?))),*];

        Arc::new(Schema::new(v))
    }};
}

struct InformationSchemaProviderWrapper {
    pub inner: InformationSchemaProvider,
}

#[async_trait]
impl SchemaProvider for InformationSchemaProviderWrapper {
    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self.inner.table_names();
        names.extend(
            InformationSchemaEmptyTable::TABLES
                .iter()
                .map(ToString::to_string),
        );
        names
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let name_lowercase = name.to_ascii_lowercase();
        if InformationSchemaEmptyTable::TABLES.contains(&name_lowercase.as_str()) {
            Some(Arc::new(InformationSchemaEmptyTable::new(name_lowercase)))
        } else {
            self.inner.table(name).await
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        InformationSchemaEmptyTable::TABLES.contains(&name) || self.inner.table_exist(name)
    }
}

#[derive(Debug)]
struct InformationSchemaEmptyTable {
    table: String,
}

impl InformationSchemaEmptyTable {
    const TABLES: [&str; 3] = [
        "referential_constraints",
        "key_column_usage",
        "table_constraints",
    ];

    fn new(table: String) -> Self {
        Self { table }
    }
}

#[async_trait]
impl TableProvider for InformationSchemaEmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match self.table.as_str() {
            "referential_constraints" => schema!({
                "constraint_catalog"        : DataType::Utf8,
                "constraint_schema"         : DataType::Utf8,
                "constraint_name"           : DataType::Utf8,
                "unique_constraint_catalog" : DataType::Utf8,
                "unique_constraint_schema"  : DataType::Utf8,
                "unique_constraint_name"    : DataType::Utf8,
                "match_option"              : DataType::Utf8,
                "update_rule"               : DataType::Utf8,
                "delete_rule"               : DataType::Utf8,
            }),
            "key_column_usage" => schema!({
                "constraint_catalog"            : DataType::Utf8,
                "constraint_schema"             : DataType::Utf8,
                "constraint_name"               : DataType::Utf8,
                "table_catalog"                 : DataType::Utf8,
                "table_schema"                  : DataType::Utf8,
                "table_name"                    : DataType::Utf8,
                "column_name"                   : DataType::Utf8,
                "ordinal_position"              : DataType::UInt32,
                "position_in_unique_constraint" : DataType::UInt32,

            }),
            "table_constraints" => schema!({
                "constraint_catalog" : DataType::Utf8,
                "constraint_schema"  : DataType::Utf8,
                "constraint_name"    : DataType::Utf8,
                "table_catalog"      : DataType::Utf8,
                "table_schema"       : DataType::Utf8,
                "table_name"         : DataType::Utf8,
                "constraint_type"    : DataType::Utf8,
                "is_deferrable"      : DataType::Utf8,
                "initially_deferred" : DataType::Utf8,
                "enforced"           : DataType::Utf8,
                "nulls_distinct"     : DataType::Utf8,
            }),
            _ => unreachable!(),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
