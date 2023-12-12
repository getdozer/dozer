pub mod json;
mod pg_catalog;
mod predicate_pushdown;

use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::array::{as_string_array, Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::not_impl_err;
use datafusion::config::ConfigOptions;
use datafusion::datasource::{DefaultTableSource, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SQLOptions, SessionState, TaskContext};
use datafusion::physical_expr::var_provider::is_system_variables;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::ast::{self, Function, FunctionArg, FunctionArgExpr};
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use datafusion::sql::{sqlparser, ResolvedTableReference, TableReference};
use datafusion::variable::{VarProvider, VarType};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, Expr, LogicalPlan, ScalarUDF, TableProviderFilterPushDown,
    TableSource, TypeSignature, WindowUDF,
};
use dozer_types::arrow::datatypes::SchemaRef;
use dozer_types::arrow::record_batch::RecordBatch;

use dozer_cache::cache::{expression::QueryExpression, CacheRecord};
use dozer_types::arrow_types::to_arrow::{map_record_to_arrow, map_to_arrow_schema};
use dozer_types::log::debug;
use dozer_types::types::Schema as DozerSchema;
use futures_util::future::try_join_all;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

use crate::api_helper::get_records;
use crate::CacheEndpoint;

use predicate_pushdown::{predicate_pushdown, supports_predicates_pushdown};

pub(crate) struct SQLExecutor {
    ctx: Arc<SessionContext>,
}

#[derive(Clone)]
pub(crate) enum PlannedStatement {
    Query(LogicalPlan),
    Statement(&'static str),
}

struct ContextResolver {
    tables: HashMap<String, Arc<dyn TableSource>>,
    state: Arc<SessionState>,
}

impl ContextResolver {
    async fn try_new_for_statement(
        state: Arc<SessionState>,
        statement: &Statement,
    ) -> Result<Self, DataFusionError> {
        let mut table_refs = state.resolve_table_references(statement)?;
        table_refs.push(TableReference::partial("information_schema", "tables"));
        table_refs.push(TableReference::partial("information_schema", "columns"));

        let mut tables = HashMap::<String, Arc<dyn TableSource>>::with_capacity(table_refs.len());
        let config = state.config_options();
        let default_catalog = &config.catalog.default_catalog;
        let default_schema = &config.catalog.default_schema;
        for table_ref in table_refs {
            let table = table_ref.table();
            let resolved = resolve_table_ref(&table_ref, default_catalog, default_schema);
            if let Entry::Vacant(v) = tables.entry(resolved.to_string()) {
                if let Ok(schema) = Self::schema_for_ref(&state, resolved) {
                    if let Some(table) = schema.table(table).await {
                        v.insert(Arc::new(DefaultTableSource::new(table)));
                    }
                }
            }
        }
        Ok(Self { tables, state })
    }

    fn schema_for_ref(
        state: &SessionState,
        resolved_ref: ResolvedTableReference<'_>,
    ) -> Result<Arc<dyn SchemaProvider>> {
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
}

impl ContextProvider for ContextResolver {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let catalog = &self.state.config_options().catalog;
        let name =
            resolve_table_ref(&name, &catalog.default_catalog, &catalog.default_schema).to_string();
        self.tables
            .get(&name)
            .ok_or_else(|| DataFusionError::Plan(format!("table '{name}' not found")))
            .cloned()
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
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(format!(
                                "PostgreSQL 9.0 (Dozer {})",
                                env!("CARGO_PKG_VERSION")
                            )))))
                        }),
                    }))
                }
                "current_database" => {
                    let catalog = self.state.config_options().catalog.default_catalog.clone();
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.current_database".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                catalog.clone(),
                            ))))
                        }),
                    }));
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
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                schema.clone(),
                            ))))
                        }),
                    }));
                }
                "pg_my_temp_schema" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_my_temp_schema".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::UInt32))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::UInt32(Some(0))))
                        }),
                    }));
                }
                "pg_is_other_temp_schema" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_is_other_temp_schema".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![
                                DataType::UInt32,
                            ]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))
                        }),
                    }));
                }
                "pg_has_role" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_has_role".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                        }),
                    }));
                }
                "has_table_privilege" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.has_table_privilege".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                        }),
                    }));
                }
                "has_column_privilege" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.has_column_privilege".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                        }),
                    }));
                }
                "has_any_column_privilege" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.has_any_column_privilege".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                        }),
                    }));
                }
                "pg_column_is_updatable" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_column_is_updatable".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))
                        }),
                    }));
                }
                "pg_relation_is_updatable" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_relation_is_updatable".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Int32))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(0))))
                        }),
                    }));
                }
                "pg_get_viewdef" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_get_viewdef".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::VariadicAny,
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("".into()))))
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
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                "".to_string(),
                            ))))
                        }),
                    }));
                }
                "format_typname" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.format_type".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![
                                DataType::Utf8,
                            ]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(move |typname| {
                            let typname = match &typname[0] {
                                ColumnarValue::Array(array) => {
                                    let array = as_string_array(array);
                                    if array.len() == 0 {
                                        None
                                    } else {
                                        Some(array.value(0))
                                    }
                                }
                                _ => unreachable!(),
                            };
                            if typname.is_none() {
                                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                            }
                            let typname = match typname.unwrap() {
                                "int2" => "smallint",
                                "int4" => "integer",
                                "int8" => "bigint",
                                "timestamp" => "timestamp without time zone",
                                "timestamptz" => "timestamp with time zone",
                                "bool" => "boolean",
                                "varchar" => "character varying",
                                "string" => "character varying",
                                "float4" => "real",
                                "float8" => "double precision",
                                "time" => "time without time zone",
                                "timetz" => "time with time zone",
                                typname => typname,
                            };
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                typname.to_string(),
                            ))))
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
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                "".to_string(),
                            ))))
                        }),
                    }));
                }
                "pg_type_is_visible" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_get_expr".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Exact(vec![
                                DataType::UInt32,
                            ]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Boolean))),
                        fun: Arc::new(move |_| {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                        }),
                    }));
                }
                "unnest" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.unnest".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Any(1),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|input| match &input[0] {
                            DataType::List(field)
                            | DataType::FixedSizeList(field, _)
                            | DataType::LargeList(field) => {
                                Ok(Arc::new(field.data_type().to_owned()))
                            }
                            // This is invalid, but we need it for system
                            // table columns, as we can't express array types
                            // yet in datafusion sql
                            _ => Ok(Arc::new(DataType::Null)),
                        }),
                        // Dummy impl
                        fun: Arc::new(|_| not_impl_err!("unnest")),
                    }));
                }
                "generate_subscripts" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.generate_subscripts".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::Any(2),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|input| {
                            if matches!(
                                input[0],
                                DataType::List(_)
                                    | DataType::FixedSizeList(_, _)
                                    | DataType::LargeList(_)
                            ) && input[1].is_integer()
                            {
                                Ok(Arc::new(DataType::UInt32))
                            } else {
                                // This is invalid, but we need it for system
                                // table columns, as we can't express array types
                                // yet in datafusion sql
                                Ok(Arc::new(DataType::Null))
                            }
                        }),
                        fun: Arc::new(|_| not_impl_err!("generate_subscripts")),
                    }));
                }
                "pg_get_constraintdef" => {
                    return Some(Arc::new(ScalarUDF {
                        name: "pg_catalog.pg_get_constraintdef".to_owned(),
                        signature: datafusion_expr::Signature {
                            type_signature: datafusion_expr::TypeSignature::OneOf(vec![
                                TypeSignature::Exact(vec![DataType::UInt32]),
                                TypeSignature::Exact(vec![DataType::UInt32, DataType::Boolean]),
                            ]),
                            volatility: datafusion_expr::Volatility::Immutable,
                        },
                        return_type: Arc::new(|_| Ok(Arc::new(DataType::Utf8))),
                        fun: Arc::new(|_| not_impl_err!("pg_get_constraintdef")),
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

fn resolve_table_ref<'a>(
    reference: &'a TableReference<'a>,
    default_catalog: &'a str,
    default_schema: &'a str,
) -> ResolvedTableReference<'a> {
    let reference = {
        if matches!(reference.schema(), Some("pg_catalog")) {
            TableReference::partial(reference.schema().unwrap(), reference.table())
        } else if reference.table().to_ascii_lowercase().starts_with("pg_") {
            TableReference::partial("pg_catalog", reference.table())
        } else {
            reference.clone()
        }
    };
    reference.resolve(default_catalog, default_schema)
}

impl SQLExecutor {
    pub async fn try_new(cache_endpoints: &[Arc<CacheEndpoint>]) -> Result<Self, DataFusionError> {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_default_catalog_and_schema("dozer", "public"),
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

        pg_catalog::create(&ctx).await?;

        Ok(Self { ctx: Arc::new(ctx) })
    }

    #[allow(unused)]
    pub fn new_empty() -> Self {
        Self {
            ctx: Arc::new(SessionContext::new()),
        }
    }

    pub async fn execute(&self, plan: LogicalPlan) -> Result<DataFrame, DataFusionError> {
        self.ctx.execute_logical_plan(plan).await
    }

    async fn parse_statement(
        &self,
        mut statement: Statement,
    ) -> Result<PlannedStatement, DataFusionError> {
        let rewrite = if let Statement::Statement(ref stmt) = statement {
            match stmt.as_ref() {
                ast::Statement::StartTransaction { .. } => {
                    return Ok(PlannedStatement::Statement("BEGIN"))
                }
                ast::Statement::Commit { .. } => return Ok(PlannedStatement::Statement("COMMIT")),
                ast::Statement::Rollback { .. } => {
                    return Ok(PlannedStatement::Statement("ROLLBACK"))
                }
                ast::Statement::SetVariable { .. } => {
                    // dbg!(stmt);
                    return Ok(PlannedStatement::Statement("SET"));
                }
                ast::Statement::ShowVariable { variable } => {
                    let variable = object_name_to_string(variable);
                    let variable = match variable.as_str() {
                        "transaction.isolation.level" => "transaction_isolation".to_string(),
                        _ => variable,
                    };
                    Some(format!("SELECT \"@@{variable}\""))
                }
                _ => None,
            }
        } else {
            None
        };

        let state = self.ctx.state();

        if let Some(query) = rewrite {
            statement = state.sql_to_statement(&query, "postgres")?;
        }

        if let Statement::Statement(statement) = &mut statement {
            sql_ast_rewrites(statement)
        };

        let context_provider =
            ContextResolver::try_new_for_statement(Arc::new(state), &statement).await?;
        let planner = SqlToRel::new(&context_provider);
        let plan = planner.statement_to_plan(statement)?;
        // Some BI tools use temporary tables. Let them
        let options = SQLOptions::new().with_allow_ddl(true).with_allow_dml(true);
        options.verify_plan(&plan)?;
        Ok(PlannedStatement::Query(plan))
    }

    pub async fn parse(&self, mut sql: &str) -> Result<Vec<PlannedStatement>, DataFusionError> {
        println!("@@ query: {sql}");
        if sql
            .to_ascii_lowercase()
            .trim_start()
            .starts_with("select character_set_name")
        {
            sql = "select 'UTF8'"
        }
        let statements =
            DFParser::parse_sql_with_dialect(sql, &sqlparser::dialect::PostgreSqlDialect {})?;
        try_join_all(
            statements
                .into_iter()
                .map(|stmt| self.parse_statement(stmt)),
        )
        .await
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

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
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
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        if var_names.len() == 1 {
            match var_names[0].as_str() {
                "@@transaction_isolation" => {
                    return Ok(ScalarValue::Utf8(Some("read committed".into())))
                }
                "@@standard_conforming_strings" => return Ok(ScalarValue::Utf8(Some("on".into()))),
                "@@lc_collate" => return Ok(ScalarValue::Utf8(Some("en_US.utf8".into()))),
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
                "@@transaction_isolation" | "@@standard_conforming_strings" | "@@lc_collate" => {
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

// SQL AST rewrites
fn sql_ast_rewrites(statement: &mut ast::Statement) {
    rewrite_sum(statement);
    rewrite_format_type(statement);
    rewrite_eq_any(statement);
    rewrite_cast_to_regclass(statement);
}

fn rewrite_cast_to_regclass(statement: &mut ast::Statement) {
    ast::visit_expressions_mut(statement, |cast: &mut ast::Expr| {
        if let ast::Expr::Cast {
            data_type: ast::DataType::Regclass,
            ..
        } = cast
        {
            *cast = ast::Expr::Value(ast::Value::Number("0".to_owned(), false));
        }
        ControlFlow::<()>::Continue(())
    });
}

// SQL AST rewirte for SUM('1') to SUM(1)
fn rewrite_sum(statement: &mut ast::Statement) {
    ast::visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Function(Function { name, args, .. }) = expr {
            let name = &name.0;
            if name.len() == 1 && name[0].value.eq_ignore_ascii_case("sum") && args.len() == 1 {
                let arg = &mut args[0];
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(value))) = arg {
                    if let ast::Value::SingleQuotedString(literal) = value {
                        if literal.parse::<i64>().is_ok() {
                            *value = ast::Value::Number(literal.clone(), false);
                            return ControlFlow::<()>::Break(());
                        }
                    }
                }
            }
        };
        ControlFlow::<()>::Continue(())
    });
}

// SQL AST rewirte for format_type(arg) to format_typname((SELECT typname FROM pg_type WHERE oid = arg))
fn rewrite_format_type(statement: &mut ast::Statement) {
    ast::visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::Function(Function { name, args, .. }) = expr {
            if name
                .0
                .last()
                .unwrap()
                .value
                .eq_ignore_ascii_case("format_type")
                && args.len() == 1
            {
                let arg = &args[0];
                let sql_expr =
                    format!("format_typname((SELECT typname FROM pg_type WHERE oid = {arg}))");
                let result = try_parse_sql_expr(&sql_expr);
                if let Ok(new_expr) = result {
                    *expr = new_expr;
                }
                return ControlFlow::<()>::Break(());
            }
        };
        ControlFlow::<()>::Continue(())
    });
}

// SQL AST rewirte for left = ANY(right) to left in (right)
fn rewrite_eq_any(statement: &mut ast::Statement) {
    ast::visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        if let ast::Expr::AnyOp {
            left,
            compare_op: ast::BinaryOperator::Eq,
            right,
        } = expr
        {
            let sql_expr = format!("{left} in ({right})");
            let result = try_parse_sql_expr(&sql_expr);
            if let Ok(new_expr) = result {
                *expr = new_expr;
            }
            return ControlFlow::<()>::Break(());
        };
        ControlFlow::<()>::Continue(())
    });
}

fn try_parse_sql_expr(sql: &str) -> Result<ast::Expr, ParserError> {
    let dialect = &sqlparser::dialect::PostgreSqlDialect {};
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let tokens = tokenizer.tokenize()?;
    let mut parser = Parser::new(dialect).with_tokens(tokens);
    parser.parse_expr()
}
