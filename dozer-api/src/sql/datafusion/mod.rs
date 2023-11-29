pub mod json;
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
use datafusion::sql::planner::{ContextProvider, ParserOptions, SqlToRel};
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

pub struct SQLExecutor {
    ctx: Arc<SessionContext>,
    lazy_init: async_once_cell::OnceCell<()>,
}

struct ContextResolver {
    tables: HashMap<String, Arc<dyn TableSource>>,
    state: Arc<SessionState>,
}

impl ContextProvider for ContextResolver {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        // if let Some(table) = PgCatalogTable::from_ref_with_state(&name, self.state.clone()) {
        //     Ok(Arc::new(DefaultTableSource::new(Arc::new(table))))
        // } else {

        let catalog = &self.state.config_options().catalog;
        let name =
            resolve_table_ref(&name, &catalog.default_catalog, &catalog.default_schema).to_string();
        self.tables
            .get(&name)
            .ok_or_else(|| DataFusionError::Plan(format!("table '{name}' not found")))
            .cloned()
        // }
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
    pub fn new(cache_endpoints: Vec<Arc<CacheEndpoint>>) -> Self {
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

        // let _df = ctx.sql("sql").await.unwrap();

        Self {
            ctx: Arc::new(ctx),
            lazy_init: Default::default(),
        }
    }

    pub async fn execute(&self, plan: LogicalPlan) -> Result<DataFrame, DataFusionError> {
        self.ctx.execute_logical_plan(plan).await
    }

    fn schema_for_ref(
        &self,
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

    async fn lazy_init(&self) -> Result<(), DataFusionError> {
        self.lazy_init
            .get_or_try_init::<DataFusionError>(self.pg_catalog_sql())
            .await?;
        Ok(())
    }

    async fn parse_statement(
        &self,
        mut statement: Statement,
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        let rewrite = if let Statement::Statement(ref stmt) = statement {
            match stmt.as_ref() {
                ast::Statement::StartTransaction { .. }
                | ast::Statement::Commit { .. }
                | ast::Statement::Rollback { .. }
                | ast::Statement::SetVariable { .. } => {
                    // dbg!(stmt);
                    return Ok(None);
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
        if let Some(query) = rewrite {
            statement = self.ctx.state().sql_to_statement(&query, "postgres")?;
        }

        match &mut statement {
            Statement::Statement(statement) => sql_ast_rewrites(statement),
            _ => (),
        };

        self.lazy_init().await?;

        let provider = self.context_provider_for_statment(&statement).await?;

        let query = SqlToRel::new_with_options(
            &provider,
            ParserOptions {
                parse_float_as_decimal: false,
                enable_ident_normalization: true,
            },
        );
        let plan = query.statement_to_plan(statement)?;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false);
        options.verify_plan(&plan)?;
        Ok(Some(plan))
    }

    async fn context_provider_for_statment(
        &self,
        statement: &Statement,
    ) -> Result<impl ContextProvider, DataFusionError> {
        let state = self.ctx.state();
        let mut table_refs = state.resolve_table_references(statement)?;
        table_refs.push(TableReference::partial("information_schema", "tables"));
        table_refs.push(TableReference::partial("information_schema", "columns"));

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
            let resolved = resolve_table_ref(&table_ref, default_catalog, default_schema);
            if let Entry::Vacant(v) = provider.tables.entry(resolved.to_string()) {
                if let Ok(schema) = self.schema_for_ref(&state, resolved) {
                    if let Some(table) = schema.table(table).await {
                        v.insert(Arc::new(DefaultTableSource::new(table)));
                    }
                }
            }
        }
        Ok(provider)
    }

    pub async fn parse(&self, mut sql: &str) -> Result<Vec<Option<LogicalPlan>>, DataFusionError> {
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

    // async fn sql_with_params(
    //     &self,
    //     sql: &str,
    //     param_values: Vec<ScalarValue>,
    // ) -> Result<(), DataFusionError> {
    //     let sql = format!("PREPARE my_plan as {sql}");
    //     let df = self.ctx.sql(&sql).await?;
    //     let _r = df.with_param_values(param_values)?.collect().await?;
    //     Ok(())
    // }

    async fn dml_sql_with_params(
        &self,
        sql: &str,
        param_values: Vec<ScalarValue>,
    ) -> Result<(), DataFusionError> {
        self.ctx
            .sql(sql)
            .await
            .map_err(|err| {
                eprintln!("error in dml query {sql}: {err}");
                err
            })?
            .with_param_values(param_values)
            .map_err(|err| {
                eprintln!("error in dml query {sql}: {err}");
                err
            })?
            .collect()
            .await
            .map_err(|err| {
                eprintln!("error in dml query {sql}: {err}");
                err
            })?;
        Ok(())
    }

    async fn pg_catalog_sql(&self) -> Result<(), DataFusionError> {
        let schema_query = "

        CREATE SCHEMA pg_catalog;

        CREATE TABLE pg_catalog.pg_authid (
            oid int unsigned NOT NULL,
            rolname string NOT NULL,
            rolsuper boolean NOT NULL,
            rolinherit boolean NOT NULL,
            rolcreaterole boolean NOT NULL,
            rolcreatedb boolean NOT NULL,
            rolcanlogin boolean NOT NULL,
            rolreplication boolean NOT NULL,
            rolbypassrls boolean NOT NULL,
            rolconnlimit integer NOT NULL,
            rolpassword text,
            rolvaliduntil timestamp with time zone,
            PRIMARY KEY (oid),
            UNIQUE (rolname),
        );

        CREATE TABLE pg_catalog.pg_tablespace (
            oid int unsigned NOT NULL,
            spcname string NOT NULL,
            spcowner int unsigned NOT NULL,
            spcacl string,
            spcoptions text,
            PRIMARY KEY (oid),
            UNIQUE (spcname),
        );

        CREATE TABLE pg_catalog.pg_database (
            oid int unsigned NOT NULL,
            datname string NOT NULL,
            datdba int unsigned NOT NULL,
            encoding integer NOT NULL,
            datlocprovider string NOT NULL,
            datistemplate boolean NOT NULL,
            datallowconn boolean NOT NULL,
            datconnlimit integer NOT NULL,
            datfrozenxid int unsigned NOT NULL,
            datminmxid int unsigned NOT NULL,
            dattablespace int unsigned NOT NULL,
            datcollate text NOT NULL,
            datctype text NOT NULL,
            daticulocale text,
            daticurules text,
            datcollversion text,
            datacl string,
            UNIQUE (datname),
            PRIMARY KEY (oid),
        );

        CREATE TABLE pg_catalog.pg_namespace (
            oid int unsigned NOT NULL,
            nspname string NOT NULL,
            nspowner int unsigned NOT NULL,
            nspacl string,
            PRIMARY KEY (oid),
            UNIQUE (nspname),
        );    
    
        CREATE TABLE pg_catalog.pg_range (
            rngtypid int unsigned NOT NULL,
            rngsubtype int unsigned NOT NULL,
            rngmultitypid int unsigned NOT NULL,
            rngcollation int unsigned NOT NULL,
            rngsubopc int unsigned NOT NULL,
            rngcanonical int unsigned NOT NULL,
            rngsubdiff int unsigned NOT NULL,
            PRIMARY KEY (rngtypid),
            UNIQUE (rngmultitypid),
        );

        CREATE TABLE pg_catalog.pg_type (
            oid int unsigned NOT NULL,
            typname string NOT NULL,
            typnamespace int unsigned NOT NULL,
            typowner int unsigned NOT NULL,
            typlen smallint NOT NULL,
            typbyval boolean NOT NULL,
            typtype char NOT NULL,
            typcategory string NOT NULL,
            typispreferred boolean NOT NULL,
            typisdefined boolean NOT NULL,
            typdelim string NOT NULL,
            typrelid int unsigned NOT NULL,
            typsubscript int unsigned NOT NULL,
            typelem int unsigned NOT NULL,
            typarray int unsigned NOT NULL,
            typinput int unsigned NOT NULL,
            typoutput int unsigned NOT NULL,
            typreceive int unsigned NOT NULL,
            typsend int unsigned NOT NULL,
            typmodin int unsigned NOT NULL,
            typmodout int unsigned NOT NULL,
            typanalyze int unsigned NOT NULL,
            typalign string NOT NULL,
            typstorage string NOT NULL,
            typnotnull boolean NOT NULL,
            typbasetype int unsigned NOT NULL,
            typtypmod integer NOT NULL,
            typndims integer NOT NULL,
            typcollation int unsigned NOT NULL,
            typdefaultbin string,
            typdefault text,
            typacl text,
            PRIMARY KEY (oid),
            UNIQUE (typname, typnamespace),
        );
        
        CREATE TABLE pg_catalog.pg_am (
            oid int unsigned NOT NULL,
            amname string NOT NULL,
            amhandler string NOT NULL,
            amtype string NOT NULL,
            UNIQUE (amname),
            PRIMARY KEY (oid),
        );

        CREATE TABLE pg_catalog.pg_attribute (
            attrelid int unsigned NOT NULL,
            attname string NOT NULL,
            atttypid int unsigned NOT NULL,
            attlen smallint NOT NULL,
            attnum smallint NOT NULL,
            attcacheoff integer NOT NULL,
            atttypmod integer NOT NULL,
            attndims smallint NOT NULL,
            attbyval boolean NOT NULL,
            attalign string NOT NULL,
            attstorage string NOT NULL,
            attcompression string NOT NULL,
            attnotnull boolean NOT NULL,
            atthasdef boolean NOT NULL,
            atthasmissing boolean NOT NULL,
            attidentity string NOT NULL,
            attgenerated string NOT NULL,
            attisdropped boolean NOT NULL,
            attislocal boolean NOT NULL,
            attinhcount smallint NOT NULL,
            attstattarget smallint NOT NULL,
            attcollation int unsigned NOT NULL,
            attacl string,
            attoptions string,
            attfdwoptions string,
            attmissingval string,
            PRIMARY KEY (attrelid, attnum),
            UNIQUE (attrelid, attname),
        );

        CREATE TABLE pg_catalog.pg_attrdef (
            oid int unsigned NOT NULL,
            adrelid int unsigned NOT NULL,
            adnum smallint NOT NULL,
            adbin string NOT NULL,
            PRIMARY KEY (oid),
            UNIQUE (adrelid, adnum),
        );

        CREATE TABLE pg_catalog.pg_proc (
            oid int unsigned NOT NULL,
            proname string NOT NULL,
            pronamespace int unsigned NOT NULL,
            proowner int unsigned NOT NULL,
            prolang int unsigned NOT NULL,
            procost real NOT NULL,
            prorows real NOT NULL,
            provariadic int unsigned NOT NULL,
            prosupport string NOT NULL,
            prokind string NOT NULL,
            prosecdef boolean NOT NULL,
            proleakproof boolean NOT NULL,
            proisstrict boolean NOT NULL,
            proretset boolean NOT NULL,
            provolatile string NOT NULL,
            proparallel string NOT NULL,
            pronargs smallint NOT NULL,
            pronargdefaults smallint NOT NULL,
            prorettype int unsigned NOT NULL,
            proargtypes string NOT NULL,
            proallargtypes string,
            proargmodes string,
            proargnames text,
            proargdefaults string,
            protrftypes string,
            prosrc text NOT NULL,
            probin text,
            prosqlbody string,
            proconfig text,
            proacl string,
            PRIMARY KEY (oid),
            UNIQUE (proname, proargtypes, pronamespace),
        );

        CREATE TABLE pg_catalog.pg_language (
            oid int unsigned NOT NULL,
            lanname string NOT NULL,
            lanowner int unsigned NOT NULL,
            lanispl boolean NOT NULL,
            lanpltrusted boolean NOT NULL,
            lanplcallfoid int unsigned NOT NULL,
            laninline int unsigned NOT NULL,
            lanvalidator int unsigned NOT NULL,
            lanacl string,
            PRIMARY KEY (oid),
            UNIQUE (lanname),
        );

        CREATE TABLE pg_catalog.pg_collation (
            oid int unsigned NOT NULL,
            collname string NOT NULL,
            collnamespace int unsigned NOT NULL,
            collowner int unsigned NOT NULL,
            collprovider string NOT NULL,
            collisdeterministic boolean NOT NULL,
            collencoding integer NOT NULL,
            collcollate text,
            collctype text,
            colliculocale text,
            collicurules text,
            collversion text,
            PRIMARY KEY (oid),
            UNIQUE (collname, collencoding, collnamespace),
        );

        CREATE TABLE pg_catalog.pg_constraint (
            oid int unsigned NOT NULL,
            conname string NOT NULL,
            connamespace int unsigned NOT NULL,
            contype string NOT NULL,
            condeferrable boolean NOT NULL,
            condeferred boolean NOT NULL,
            convalidated boolean NOT NULL,
            conrelid int unsigned NOT NULL,
            contypid int unsigned NOT NULL,
            conindid int unsigned NOT NULL,
            conparentid int unsigned NOT NULL,
            confrelid int unsigned NOT NULL,
            confupdtype string NOT NULL,
            confdeltype string NOT NULL,
            confmatchtype string NOT NULL,
            conislocal boolean NOT NULL,
            coninhcount smallint NOT NULL,
            connoinherit boolean NOT NULL,
            conkey string,
            confkey string,
            conpfeqop string,
            conppeqop string,
            conffeqop string,
            confdelsetcols string,
            conexclop string,
            conbin string,
            PRIMARY KEY (oid),
            UNIQUE (conrelid, contypid, conname),
        );

        CREATE TABLE pg_catalog.pg_rewrite (
            oid int unsigned NOT NULL,
            rulename string NOT NULL,
            ev_class int unsigned NOT NULL,
            ev_type string NOT NULL,
            ev_enabled string NOT NULL,
            is_instead boolean NOT NULL,
            ev_qual string NOT NULL,
            ev_action string NOT NULL,
            PRIMARY KEY (oid),
            UNIQUE (ev_class, rulename),
        );

        CREATE TABLE pg_catalog.pg_trigger (
            oid int unsigned NOT NULL,
            tgrelid int unsigned NOT NULL,
            tgparentid int unsigned NOT NULL,
            tgname string NOT NULL,
            tgfoid int unsigned NOT NULL,
            tgtype smallint NOT NULL,
            tgenabled string NOT NULL,
            tgisinternal boolean NOT NULL,
            tgconstrrelid int unsigned NOT NULL,
            tgconstrindid int unsigned NOT NULL,
            tgconstraint int unsigned NOT NULL,
            tgdeferrable boolean NOT NULL,
            tginitdeferred boolean NOT NULL,
            tgnargs smallint NOT NULL,
            tgattr string NOT NULL,
            tgargs string NOT NULL,
            tgqual string,
            tgoldtable string,
            tgnewtable string,
            PRIMARY KEY (oid),
            UNIQUE (tgrelid, tgname),
        );

        CREATE TABLE pg_catalog.pg_operator (
            oid int unsigned NOT NULL,
            oprname string NOT NULL,
            oprnamespace int unsigned NOT NULL,
            oprowner int unsigned NOT NULL,
            oprkind string NOT NULL,
            oprcanmerge boolean NOT NULL,
            oprcanhash boolean NOT NULL,
            oprleft int unsigned NOT NULL,
            oprright int unsigned NOT NULL,
            oprresult int unsigned NOT NULL,
            oprcom int unsigned NOT NULL,
            oprnegate int unsigned NOT NULL,
            oprcode string NOT NULL,
            oprrest string NOT NULL,
            oprjoin string NOT NULL,
            PRIMARY KEY (oid),
            UNIQUE (oprname, oprleft, oprright, oprnamespace),
        );        

        CREATE TABLE pg_catalog.pg_policy (
            oid int unsigned NOT NULL,
            polname string NOT NULL,
            polrelid int unsigned NOT NULL,
            polcmd string NOT NULL,
            polpermissive boolean NOT NULL,
            polroles string NOT NULL,
            polqual string,
            polwithcheck string,
            PRIMARY KEY (oid),
            UNIQUE (polrelid, polname),
        );

        CREATE TABLE pg_catalog.pg_depend (
            classid int unsigned NOT NULL,
            objid int unsigned NOT NULL,
            objsubid integer NOT NULL,
            refclassid int unsigned NOT NULL,
            refobjid int unsigned NOT NULL,
            refobjsubid integer NOT NULL,
            deptype char NOT NULL,
        );

        CREATE TABLE pg_catalog.pg_description (
            objoid int unsigned NOT NULL,
            classoid int unsigned NOT NULL,
            objsubid integer NOT NULL,
            description text NOT NULL,
            PRIMARY KEY (objoid, classoid, objsubid),
        );

        CREATE TABLE pg_catalog.pg_enum (
            oid int unsigned NOT NULL,
            enumtypid int unsigned NOT NULL,
            enumsortorder real NOT NULL,
            enumlabel string NOT NULL,
            PRIMARY KEY (oid),
            UNIQUE (enumtypid, enumlabel),
            UNIQUE (enumtypid, enumsortorder),
        );

        CREATE TABLE pg_catalog.pg_index (
            indexrelid int unsigned NOT NULL,
            indrelid int unsigned NOT NULL,
            indnatts smallint NOT NULL,
            indnkeyatts smallint NOT NULL,
            indisunique boolean NOT NULL,
            indnullsnotdistinct boolean NOT NULL,
            indisprimary boolean NOT NULL,
            indisexclusion boolean NOT NULL,
            indimmediate boolean NOT NULL,
            indisclustered boolean NOT NULL,
            indisvalid boolean NOT NULL,
            indcheckxmin boolean NOT NULL,
            indisready boolean NOT NULL,
            indislive boolean NOT NULL,
            indisreplident boolean NOT NULL,
            indkey string NOT NULL,
            indcollation string NOT NULL,
            indclass string NOT NULL,
            indoption string NOT NULL,
            indexprs string,
            indpred string,
            PRIMARY KEY (indexrelid),
        );

        CREATE TABLE pg_catalog.pg_class (
            oid int unsigned NOT NULL,
            relname string NOT NULL,
            relnamespace int unsigned NOT NULL,
            reltype int unsigned NOT NULL,
            reloftype int unsigned NOT NULL,
            relowner int unsigned NOT NULL,
            relam int unsigned NOT NULL,
            relfilenode int unsigned NOT NULL,
            reltablespace int unsigned NOT NULL,
            relpages int NOT NULL,
            reltuples real NOT NULL,
            relallvisible int NOT NULL,
            reltoastrelid int unsigned NOT NULL,
            relhasindex boolean NOT NULL,
            relisshared boolean NOT NULL,
            relpersistence string NOT NULL,
            relkind string NOT NULL,
            relnatts smallint NOT NULL,
            relchecks smallint NOT NULL,
            relhasrules boolean NOT NULL,
            relhastriggers boolean NOT NULL,
            relhassubclass boolean NOT NULL,
            relrowsecurity boolean NOT NULL,
            relforcerowsecurity boolean NOT NULL,
            relispopulated boolean NOT NULL,
            relreplident string NOT NULL,
            relispartition boolean NOT NULL,
            relrewrite int unsigned NOT NULL,
            relfrozenxid int unsigned NOT NULL,
            relminmxid int unsigned NOT NULL,
            relacl string,
            reloptions string,
            relpartbound string,
            PRIMARY KEY (oid),
            UNIQUE (relname, relnamespace),
        );


        CREATE SCHEMA information_schema;

        CREATE VIEW information_schema.tables AS
        SELECT (current_database()) AS table_catalog,
           (nc.nspname) AS table_schema,
           (c.relname) AS table_name,
           (
               CASE
                   WHEN (nc.oid = pg_my_temp_schema()) THEN 'LOCAL TEMPORARY'
                   WHEN (c.relkind in ('r', 'p')) THEN 'BASE TABLE'
                   WHEN (c.relkind = 'v') THEN 'VIEW'
                   WHEN (c.relkind = 'f') THEN 'FOREIGN'
                   ELSE NULL
               END) AS table_type,
           (NULL) AS self_referencing_column_name,
           (NULL) AS reference_generation,
           (
               CASE
                   WHEN (t.typname IS NOT NULL) THEN current_database()
                   ELSE NULL
               END) AS user_defined_type_catalog,
           (nt.nspname) AS user_defined_type_schema,
           (t.typname) AS user_defined_type_name,
           ('NO') AS is_insertable_into,
           (
               CASE
                   WHEN (t.typname IS NOT NULL) THEN 'YES'
                   ELSE 'NO'
               END) AS is_typed,
           (NULL) AS commit_action
          FROM ((pg_namespace nc
            JOIN pg_class c ON ((nc.oid = c.relnamespace)))
            LEFT JOIN (pg_type t
            JOIN pg_namespace nt ON ((t.typnamespace = nt.oid))) ON ((c.reloftype = t.oid)))
           WHERE ((c.relkind in ('r', 'v', 'f', 'p'))
               AND (NOT pg_is_other_temp_schema(nc.oid))
               AND (pg_has_role(c.relowner, 'USAGE')
                    OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                    OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES')));


        CREATE VIEW information_schema.columns AS
        SELECT (current_database()) AS table_catalog,
            (nc.nspname) AS table_schema,
            (c.relname) AS table_name,
            (a.attname) AS column_name,
            (a.attnum) AS ordinal_position,
            (
                CASE
                    WHEN (a.attgenerated = '') THEN pg_get_expr(ad.adbin, ad.adrelid)
                    ELSE NULL
                END) AS column_default,
            (
                CASE
                    WHEN (a.attnotnull OR ((t.typtype = 'd') AND t.typnotnull)) THEN 'NO'
                    ELSE 'YES'
                END) AS is_nullable,
            (
                CASE
                    WHEN (t.typtype = 'd') THEN
                    CASE
                        WHEN ((bt.typelem <> (0)) AND (bt.typlen = '-1')) THEN 'ARRAY'
                        WHEN (nbt.nspname = 'pg_catalog') THEN format_typname((SELECT typname FROM pg_type WHERE oid = t.typbasetype))
                        ELSE 'USER-DEFINED'
                    END
                    ELSE
                    CASE
                        WHEN ((t.typelem <> (0)) AND (t.typlen = '-1')) THEN 'ARRAY'
                        WHEN (nt.nspname = 'pg_catalog') THEN format_typname((SELECT typname FROM pg_type WHERE oid = a.atttypid))
                        ELSE 'USER-DEFINED'
                    END
                END) AS data_type,
            (NULL::integer) AS character_maximum_length,
            (NULL::integer) AS character_octet_length,
            (NULL::integer) AS numeric_precision,
            (NULL::integer) AS numeric_precision_radix,
            (NULL::integer) AS numeric_scale,
            (NULL::integer) AS datetime_precision,
            (NULL::integer) AS interval_type,
            (NULL::integer) AS interval_precision,
            (NULL::string) AS character_set_catalog,
            (NULL::string) AS character_set_schema,
            (NULL::string) AS character_set_name,
            (
                CASE
                    WHEN (nco.nspname IS NOT NULL) THEN current_database()
                    ELSE NULL::string
                END) AS collation_catalog,
            (nco.nspname) AS collation_schema,
            (co.collname) AS collation_name,
            (
                CASE
                    WHEN (t.typtype = 'd') THEN current_database()
                    ELSE NULL::string
                END) AS domain_catalog,
            (
                CASE
                    WHEN (t.typtype = 'd') THEN nt.nspname
                    ELSE NULL::string
                END) AS domain_schema,
            (
                CASE
                    WHEN (t.typtype = 'd') THEN t.typname
                    ELSE NULL::string
                END) AS domain_name,
            (current_database()) AS udt_catalog,
            (COALESCE(nbt.nspname, nt.nspname)) AS udt_schema,
            (COALESCE(bt.typname, t.typname)) AS udt_name,
            (NULL::string) AS scope_catalog,
            (NULL::string) AS scope_schema,
            (NULL::string) AS scope_name,
            (NULL::integer) AS maximum_cardinality,
            (a.attnum) AS dtd_identifier,
            ('NO') AS is_self_referencing,
            (
                CASE
                    WHEN (a.attidentity in ('a', 'd')) THEN 'YES'
                    ELSE 'NO'
                END) AS is_identity,
            (
                CASE a.attidentity
                    WHEN 'a' THEN 'ALWAYS'
                    WHEN 'd' THEN 'BY DEFAULT'
                    ELSE NULL
                END) AS identity_generation,
            (
                CASE
                    WHEN (a.attgenerated <> '') THEN 'ALWAYS'
                    ELSE 'NEVER'
                END) AS is_generated,
            (
                CASE
                    WHEN (a.attgenerated <> '') THEN pg_get_expr(ad.adbin, ad.adrelid)
                    ELSE NULL
                END) AS generation_expression,
            (
                CASE
                    WHEN ((c.relkind in ('r', 'p')) OR ((c.relkind in ('v', 'f')) AND pg_column_is_updatable((c.oid), a.attnum, false))) THEN 'YES'
                    ELSE 'NO'
                END) AS is_updatable
            FROM ((((((pg_attribute a
            LEFT JOIN pg_attrdef ad ON (((a.attrelid = ad.adrelid) AND (a.attnum = ad.adnum))))
            JOIN (pg_class c
            JOIN pg_namespace nc ON ((c.relnamespace = nc.oid))) ON ((a.attrelid = c.oid)))
            JOIN (pg_type t
            JOIN pg_namespace nt ON ((t.typnamespace = nt.oid))) ON ((a.atttypid = t.oid)))
            LEFT JOIN (pg_type bt
            JOIN pg_namespace nbt ON ((bt.typnamespace = nbt.oid))) ON (((t.typtype = 'd') AND (t.typbasetype = bt.oid))))
            LEFT JOIN (pg_collation co
            JOIN pg_namespace nco ON ((co.collnamespace = nco.oid))) ON (((a.attcollation = co.oid) AND ((nco.nspname <> 'pg_catalog') OR (co.collname <> 'default'))))))
            WHERE ((NOT pg_is_other_temp_schema(nc.oid))
                AND (a.attnum > 0)
                AND (NOT a.attisdropped)
                AND (c.relkind in ('r', 'v', 'f', 'p'))
                AND (pg_has_role(c.relowner, 'USAGE')
                     OR has_column_privilege(c.oid, a.attnum, 'SELECT, INSERT, UPDATE, REFERENCES')));


        CREATE VIEW information_schema.views AS
        SELECT (current_database()) AS table_catalog,
        (nc.nspname) AS table_schema,
        (c.relname) AS table_name,
        (
            CASE
                WHEN pg_has_role(c.relowner, 'USAGE'::text) THEN pg_get_viewdef(c.oid)
                ELSE NULL::text
            END) AS view_definition,
        (
            CASE
                WHEN ('check_option=cascaded'::text in (c.reloptions)) THEN 'CASCADED'::text
                WHEN ('check_option=local'::text in (c.reloptions)) THEN 'LOCAL'::text
                ELSE 'NONE'::text
            END) AS check_option,
        (
            CASE
                WHEN ((pg_relation_is_updatable((c.oid), false) & 20) = 20) THEN 'YES'::text
                ELSE 'NO'::text
            END) AS is_updatable,
        (
            CASE
                WHEN ((pg_relation_is_updatable((c.oid), false) & 8) = 8) THEN 'YES'::text
                ELSE 'NO'::text
            END) AS is_insertable_into,
        ('NO'::text) AS is_trigger_updatable,
        ('NO'::text) AS is_trigger_deletable,
        ('NO'::text) AS is_trigger_insertable_into
        FROM pg_namespace nc,
        pg_class c
        WHERE ((c.relnamespace = nc.oid)
        AND (c.relkind = 'v'::char)
        AND (NOT pg_is_other_temp_schema(nc.oid))
        AND (pg_has_role(c.relowner, 'USAGE'::text)
                OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'::text)
                OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'::text)));


        CREATE VIEW information_schema.referential_constraints AS
        SELECT (current_database()) AS constraint_catalog,
            (ncon.nspname) AS constraint_schema,
            (con.conname) AS constraint_name,
            (
                CASE
                    WHEN (npkc.nspname IS NULL) THEN NULL::string
                    ELSE current_database()
                END) AS unique_constraint_catalog,
            (npkc.nspname) AS unique_constraint_schema,
            (pkc.conname) AS unique_constraint_name,
            (
                CASE con.confmatchtype
                    WHEN 'f'::char THEN 'FULL'::text
                    WHEN 'p'::char THEN 'PARTIAL'::text
                    WHEN 's'::char THEN 'NONE'::text
                    ELSE NULL::text
                END) AS match_option,
            (
                CASE con.confupdtype
                    WHEN 'c'::char THEN 'CASCADE'::text
                    WHEN 'n'::char THEN 'SET NULL'::text
                    WHEN 'd'::char THEN 'SET DEFAULT'::text
                    WHEN 'r'::char THEN 'RESTRICT'::text
                    WHEN 'a'::char THEN 'NO ACTION'::text
                    ELSE NULL::text
                END) AS update_rule,
            (
                CASE con.confdeltype
                    WHEN 'c'::char THEN 'CASCADE'::text
                    WHEN 'n'::char THEN 'SET NULL'::text
                    WHEN 'd'::char THEN 'SET DEFAULT'::text
                    WHEN 'r'::char THEN 'RESTRICT'::text
                    WHEN 'a'::char THEN 'NO ACTION'::text
                    ELSE NULL::text
                END) AS delete_rule
            FROM ((((((pg_namespace ncon
            JOIN pg_constraint con ON ((ncon.oid = con.connamespace)))
            JOIN pg_class c ON (((con.conrelid = c.oid) AND (con.contype = 'f'::char))))
            LEFT JOIN pg_depend d1 ON (((d1.objid = con.oid) AND (d1.classid = (SELECT oid FROM pg_class WHERE relname = 'pg_constraint')) AND (d1.refclassid = (SELECT oid FROM pg_class WHERE relname = 'pg_class')) AND (d1.refobjsubid = 0))))
            LEFT JOIN pg_depend d2 ON (((d2.refclassid = (SELECT oid FROM pg_class WHERE relname = 'pg_constraint')) AND (d2.classid = (SELECT oid FROM pg_class WHERE relname = 'pg_class')) AND (d2.objid = d1.refobjid) AND (d2.objsubid = 0) AND (d2.deptype = 'i'::char))))
            LEFT JOIN pg_constraint pkc ON (((pkc.oid = d2.refobjid) AND (pkc.contype in ('p'::char, 'u'::char)) AND (pkc.conrelid = con.confrelid))))
            LEFT JOIN pg_namespace npkc ON ((pkc.connamespace = npkc.oid)))
            WHERE (pg_has_role(c.relowner, 'USAGE'::text)
                OR has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'::text)
                OR has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'::text));

        CREATE TABLE information_schema.key_column_usage(
            constraint_catalog string,
            constraint_schema string,
            constraint_name string,
            table_catalog string,
            table_schema string,
            table_name string,
            column_name string,
            ordinal_position int unsigned,
            position_in_unique_constraint int unsigned,
        );

        CREATE TABLE information_schema.table_constraints(
            constraint_catalog string,
            constraint_schema string,
            constraint_name string,
            table_catalog string,
            table_schema string,
            table_name string,
            constraint_type string,
            is_deferrable string,
            initially_deferred string,
            enforced string,
            nulls_distinct string,
        );

        ";

        let state = self.ctx.state();

        {
            let statements = DFParser::parse_sql_with_dialect(
                schema_query,
                &sqlparser::dialect::PostgreSqlDialect {},
            )?;
            for statement in statements {
                let provider = self.context_provider_for_statment(&statement).await?;
                let query = SqlToRel::new(&provider);
                let plan = query.statement_to_plan(statement)?;
                self.ctx.execute_logical_plan(plan).await?;
            }
        }

        // data
        {
            struct OIDFactory(u32);
            impl OIDFactory {
                fn new() -> Self {
                    Self(1)
                }

                fn next(&mut self) -> u32 {
                    let oid = self.0;
                    self.0 += 1;
                    oid
                }
            }

            let mut oids = OIDFactory::new();

            let owner_id = oids.next();
            let owner_name = "dozer";
            self.dml_sql_with_params(
                "INSERT
                INTO pg_catalog.pg_authid
                VALUES ($1, $2, true, true, true, true, true, true, true, -1, null, null)",
                vec![owner_id.into(), owner_name.into()],
            )
            .await?;

            let default_table_space_id = oids.next();
            let default_table_space_name = "pg_default";
            self.dml_sql_with_params(
                "INSERT INTO pg_catalog.pg_tablespace VALUES($1, $2, $3, null, null)",
                vec![
                    default_table_space_id.into(),
                    default_table_space_name.into(),
                    owner_id.into(),
                ],
            )
            .await?;

            struct SQLType {
                pub id: u32,
                pub name: &'static str,
                pub len: i16,
                pub category_code: &'static str,
                pub preferred: bool,
            }

            impl SQLType {
                fn new(
                    id: u32,
                    name: &'static str,
                    len: i16,
                    category_code: &'static str,
                    preferred: bool,
                ) -> Self {
                    Self {
                        id,
                        name,
                        len,
                        category_code,
                        preferred,
                    }
                }
            }

            let sqltypes = [
                SQLType::new(oids.next(), "bool", 1i16, "B", true),
                SQLType::new(oids.next(), "bytea", -1, "U", false),
                SQLType::new(oids.next(), "char", 1, "Z", false),
                SQLType::new(oids.next(), "int2", 2, "N", false),
                SQLType::new(oids.next(), "int4", 4, "N", false),
                SQLType::new(oids.next(), "int8", 8, "N", false),
                SQLType::new(oids.next(), "varchar", -1, "S", false),
                SQLType::new(oids.next(), "text", -1, "S", true),
                SQLType::new(oids.next(), "float4", 4, "N", false),
                SQLType::new(oids.next(), "float8", 8, "N", true),
                SQLType::new(oids.next(), "date", 4, "D", false),
                SQLType::new(oids.next(), "time", 8, "D", false),
                SQLType::new(oids.next(), "timestamp", 8, "D", false),
                SQLType::new(oids.next(), "timestamptz", 8, "D", true),
                SQLType::new(oids.next(), "interval", 16, "T", true),
                SQLType::new(oids.next(), "numeric", -1, "N", false),
                SQLType::new(oids.next(), "unknown", -2, "X", false),
            ];

            let sqltype_by_name =
                |name| -> &SQLType { sqltypes.iter().find(|&t| t.name.eq(name)).unwrap() };

            let sqltype_for_dftype = |dftype: &DataType| -> &SQLType {
                match dftype {
                    DataType::Null => sqltype_by_name("unknown"),
                    DataType::Boolean => sqltype_by_name("bool"),
                    DataType::Int8 => sqltype_by_name("char"),
                    DataType::Int16 => sqltype_by_name("int2"),
                    DataType::Int32 => sqltype_by_name("int4"),
                    DataType::Int64 => sqltype_by_name("int8"),
                    DataType::UInt8 => sqltype_by_name("char"),
                    DataType::UInt16 => sqltype_by_name("int2"),
                    DataType::UInt32 => sqltype_by_name("int4"),
                    DataType::UInt64 => sqltype_by_name("int8"),
                    DataType::Float16 => sqltype_by_name("float4"),
                    DataType::Float32 => sqltype_by_name("float4"),
                    DataType::Float64 => sqltype_by_name("float8"),
                    DataType::Timestamp(_, None) => sqltype_by_name("timestamp"),
                    DataType::Timestamp(_, Some(_)) => sqltype_by_name("timestamptz"),
                    DataType::Date32 => sqltype_by_name("date"),
                    DataType::Date64 => sqltype_by_name("date"),
                    DataType::Time32(_) => sqltype_by_name("time"),
                    DataType::Time64(_) => sqltype_by_name("time"),
                    DataType::Duration(_) => sqltype_by_name("interval"),
                    DataType::Interval(_) => sqltype_by_name("interval"),
                    DataType::Binary => sqltype_by_name("bytea"),
                    DataType::FixedSizeBinary(_) => sqltype_by_name("bytea"),
                    DataType::LargeBinary => sqltype_by_name("bytea"),
                    DataType::Utf8 => sqltype_by_name("text"),
                    DataType::LargeUtf8 => sqltype_by_name("text"),
                    DataType::Decimal256(_, _) | DataType::Decimal128(_, _) => {
                        sqltype_by_name("numeric")
                    }
                    DataType::List(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::LargeList(_)
                    | DataType::Struct(_)
                    | DataType::Union(_, _)
                    | DataType::Dictionary(_, _)
                    | DataType::Map(_, _)
                    | DataType::RunEndEncoded(_, _) => unimplemented!("No SQLType for {dftype}"),
                }
            };

            let pg_catalog_schema_id = oids.next();
            self.dml_sql_with_params(
                "INSERT INTO pg_catalog.pg_namespace VALUES($1, 'pg_catalog', $2, null)",
                vec![pg_catalog_schema_id.into(), owner_id.into()],
            )
            .await?;

            self.dml_sql_with_params(
                "INSERT INTO pg_catalog.pg_proc VALUES(1, 'dummy', $1, $2, 0, 0, 0, 0, '', '', false, false, false, false, '', '', 0, 0, 0, '', null, null, null, null, null, '', null, null, null, null)",
                vec![pg_catalog_schema_id.into(), owner_id.into()],
            )
            .await?;

            for sqltype in sqltypes.iter() {
                let pass_by_val = sqltype.len != -1;
                self.dml_sql_with_params(
                    "INSERT INTO pg_catalog.pg_type
                    VALUES($1,      /* oid */
                           $2,      /* typname */         
                           $3,      /* typnamespace */         
                           $4,      /* typowner */         
                           $5,      /* typlen */         
                           $6,      /* typbyval */         
                           'b',     /* typtype */          
                           $7,      /* typcategory */         
                           $8,      /* typispreferred */         
                           true,    /* typisdefined */           
                           '',      /* typdelim */         
                           0,       /* typrelid */        
                           0,       /* typsubscript */        
                           0,       /* typelem */        
                           0,       /* typarray */        
                           0,       /* typinput */        
                           0,       /* typoutput */        
                           1,       /* typreceive */        
                           0,       /* typsend */        
                           0,       /* typmodin */        
                           0,       /* typmodout */        
                           0,       /* typanalyze */        
                           'c',     /* typalign */          
                           'p',     /* typstorage */          
                           false,   /* typnotnull */            
                           0,       /* typbasetype */        
                           0,       /* typtypmod */        
                           0,       /* typndims */        
                           0,       /* typcollation */        
                           null,    /* typdefaultbin */           
                           null,    /* typdefault */           
                           null     /* typacl */          
                    )",
                    vec![
                        sqltype.id.into(),
                        sqltype.name.into(),
                        pg_catalog_schema_id.into(),
                        owner_id.into(),
                        sqltype.len.into(),
                        pass_by_val.into(),
                        sqltype.category_code.into(),
                        sqltype.preferred.into(),
                    ],
                )
                .await?;
            }

            let catalog_list = state.catalog_list();
            for catalog_name in catalog_list.catalog_names() {
                let catalog_id = oids.next();
                self.dml_sql_with_params(
                    "INSERT INTO pg_catalog.pg_database
                     VALUES($1, $2, $3, 0, 'c', false, true, -1, 0, 0, $4, 'C.utf8', 'C.utf8', null, null, null, null)",
                    vec![catalog_id.into(), catalog_name.as_str().into(), owner_id.into(), default_table_space_id.into()],
                )
                .await?;

                let catalog = catalog_list.catalog(&catalog_name).unwrap();
                for schema_name in catalog.schema_names().into_iter() {
                    let schema_id = oids.next();
                    self.dml_sql_with_params(
                        "INSERT INTO pg_catalog.pg_namespace VALUES($1, $2, $3, null)",
                        vec![
                            schema_id.into(),
                            schema_name.as_str().into(),
                            owner_id.into(),
                        ],
                    )
                    .await?;

                    let schema = catalog.schema(&schema_name).unwrap();
                    for table_name in schema.table_names() {
                        let table = schema.table(&table_name).await.unwrap();
                        let table_id = oids.next();
                        self.dml_sql_with_params(
                            "INSERT INTO pg_catalog.pg_class
                        VALUES($1, $2, $3, 0, 0, $4, 0, 0, 0, 0, -1, -1, 0, false, false, 'p', 'r', $5, 0, false, false, false, false, false, true, 'd', false, 0, 1, 1, null, null, null)",
                            vec![
                                table_id.into(),
                                table_name.as_str().into(),
                                schema_id.into(),
                                owner_id.into(),
                                (table.schema().fields().len() as i16).into(),
                            ],
                        )
                        .await?;

                        for (i, field) in table.schema().fields().into_iter().enumerate() {
                            let sqltype = sqltype_for_dftype(field.data_type());
                            let ordinal = i as i16 + 1;
                            let pass_by_val = sqltype.len != -1;
                            self.dml_sql_with_params(
                                "INSERT INTO pg_catalog.pg_attribute
                            VALUES($1, $2, $3, $4, $5, -1, -1, 0, $6, 'b', 'p', '', $7, false, false, '', '', false, true, 0, 0, 0, null, null, null, null)",
                                vec![
                                    table_id.into(),
                                    field.name().as_str().into(),
                                    sqltype.id.into(),
                                    sqltype.len.into(),
                                    ordinal.into(),
                                    pass_by_val.into(),
                                    (!field.is_nullable()).into(),
                                ],
                            )
                            .await?;
                        }
                    }
                }
            }
        }

        Ok(())
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

// SQL AST rewrites
fn sql_ast_rewrites(statement: &mut ast::Statement) {
    rewrite_sum(statement);
    rewrite_format_type(statement);
}

// SQL AST rewirte for SUM('1') to SUM(1)
fn rewrite_sum(statement: &mut ast::Statement) {
    ast::visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        match expr {
            ast::Expr::Function(Function { name, args, .. }) => {
                let name = &name.0;
                if name.len() == 1 && name[0].value.eq_ignore_ascii_case("sum") {
                    if args.len() == 1 {
                        let arg = &mut args[0];
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                                value,
                            ))) => {
                                if let ast::Value::SingleQuotedString(literal) = value {
                                    if literal.parse::<i64>().is_ok() {
                                        *value = ast::Value::Number(literal.clone(), false);
                                        return ControlFlow::<()>::Break(());
                                    }
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
            _ => (),
        };
        ControlFlow::<()>::Continue(())
    });
}

// SQL AST rewirte for format_type(arg) to format_typname((SELECT typname FROM pg_type WHERE oid = arg))
fn rewrite_format_type(statement: &mut ast::Statement) {
    ast::visit_expressions_mut(statement, |expr: &mut ast::Expr| {
        match expr {
            ast::Expr::Function(Function { name, args, .. }) => {
                if name
                    .0
                    .last()
                    .unwrap()
                    .value
                    .eq_ignore_ascii_case("format_type")
                {
                    if args.len() >= 1 {
                        let arg = &args[0];
                        let sql_expr = format!(
                            "format_typname((SELECT typname FROM pg_type WHERE oid = {arg}))"
                        );
                        let result = try_parse_sql_expr(&sql_expr);
                        if let Ok(new_expr) = result {
                            *expr = new_expr;
                        }
                        return ControlFlow::<()>::Break(());
                    }
                }
            }
            _ => (),
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
