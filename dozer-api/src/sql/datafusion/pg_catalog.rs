use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{StringBuilder, UInt32Builder},
        datatypes::SchemaRef,
        record_batch::RecordBatch,
    },
    datasource::TableProvider,
    error::Result,
    execution::context::SessionState,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    sql::TableReference,
};
use datafusion_expr::{Expr, TableType};

#[derive(Debug)]
pub struct PgCatalogTable {
    table: String,
    schema: SchemaRef,
    state: Arc<SessionState>,
}

impl PgCatalogTable {
    pub fn from_ref_with_state(
        reference: &TableReference,
        state: Arc<SessionState>,
    ) -> Option<Self> {
        match reference.schema() {
            Some("pg_catalog") | None => (),
            _ => return None,
        }
        let table = reference.table();
        let schema = match table {
            "pg_type" => schemas::pg_type(),
            "pg_namespace" => schemas::pg_namespace(),
            "pg_proc" => schemas::pg_proc(),
            "pg_class" => schemas::pg_class(),
            "pg_attribute" => schemas::pg_attribute(),
            "pg_description" => schemas::pg_description(),
            "pg_attrdef" => schemas::pg_attrdef(),
            _ => return None,
        };
        let table = table.to_string();
        Some(Self {
            table,
            schema,
            state: state,
        })
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
        TableType::View
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
            &[self.records()],
            self.schema.clone(),
            projection.cloned(),
        )?))
    }
}

impl PgCatalogTable {
    fn records(&self) -> Vec<RecordBatch> {
        match self.table.as_str() {
            "pg_namespace" => self.pg_namespace(),
            "pg_class" => self.pg_class(),
            _ => vec![],
        }
    }

    fn pg_namespace(&self) -> Vec<RecordBatch> {
        let mut oid = UInt32Builder::new();
        let mut nspname = StringBuilder::new();
        let mut nspowner = UInt32Builder::new();
        let mut nspacl = StringBuilder::new();

        for (i, catalog) in self
            .state
            .catalog_list()
            .catalog_names()
            .into_iter()
            .enumerate()
        {
            oid.append_value(i as u32);
            nspname.append_value(catalog);
            nspowner.append_value(0);
            nspacl.append_null();
        }

        let schema = self.schema.clone();
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oid.finish()),
                Arc::new(nspname.finish()),
                Arc::new(nspowner.finish()),
                Arc::new(nspacl.finish()),
            ],
        )
        .unwrap()]
    }

    fn pg_class(&self) -> Vec<RecordBatch> {
        let mut oid = UInt32Builder::new();
        let mut relname = StringBuilder::new();
        let mut relnamespace = UInt32Builder::new();
        let mut relkind = StringBuilder::new();

        for (i, table) in self
            .state
            .catalog_list()
            .catalog("public")
            .unwrap()
            .schema("dozer")
            .unwrap()
            .table_names()
            .into_iter()
            .enumerate()
        {
            oid.append_value(i as u32);
            relname.append_value(table);
            relnamespace.append_value(0);
            relkind.append_value("r");
        }

        let schema = self.schema.clone();
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oid.finish()),
                Arc::new(relname.finish()),
                Arc::new(relnamespace.finish()),
                Arc::new(relkind.finish()),
            ],
        )
        .unwrap()]
    }
}

pub mod schemas {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

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

    pub fn pg_type() -> Arc<Schema> {
        schema!({
            "oid"               : DataType::Utf8,
            "typname"           : DataType::Utf8,
            "typnamespace"      : DataType::Utf8,
            "typowner"          : DataType::Utf8,
            "typlen"            : DataType::Int16,
            "typbyval"          : DataType::Boolean,
            "typtype"           : DataType::Utf8,
            "typcategory"       : DataType::Utf8,
            "typispreferred"    : DataType::Boolean,
            "typisdefined"      : DataType::Boolean,
            "typdelim"          : DataType::Utf8,
            "typrelid"          : DataType::Utf8,
            "typelem"           : DataType::Utf8,
            "typarray"          : DataType::Utf8,
            "typinput"          : DataType::Utf8,
            "typoutput"         : DataType::Utf8,
            "typreceive"        : DataType::Utf8,
            "typsend"           : DataType::Utf8,
            "typmodin"          : DataType::Utf8,
            "typmodout"         : DataType::Utf8,
            "typanalyze"        : DataType::Utf8,
            "typalign"          : DataType::Utf8,
            "typstorage"        : DataType::Utf8,
            "typnotnull"        : DataType::Boolean,
            "typbasetype"       : DataType::Utf8,
            "typtypmod"         : DataType::Int32,
            "typndims"          : DataType::Int32,
            "typcollation"      : DataType::Utf8,
            "typdefaultbin"     : DataType::Binary : nullable,
            "typdefault"        : DataType::Utf8 : nullable,
            "typacl"            : DataType::Utf8 : nullable,
        })
    }
    pub fn pg_namespace() -> Arc<Schema> {
        schema!({
            "oid"       : DataType::UInt32,
            "nspname"   : DataType::Utf8,
            "nspowner"  : DataType::UInt32,
            "nspacl"    : DataType::Utf8 : nullable,
        })
    }

    pub fn pg_proc() -> Arc<Schema> {
        schema!({
             "oid"             : DataType::UInt32,
             "proname"         : DataType::Utf8,
             "pronamespace"    : DataType::UInt32,
             "proowner"        : DataType::UInt32,
             "prolang"         : DataType::UInt32,
             "procost"         : DataType::Float64,
             "prorows"         : DataType::Float64,
             "provariadic"     : DataType::UInt32,
             "prosupport"      : DataType::UInt32,
             "prokind"         : DataType::Utf8,
             "prosecdef"       : DataType::Boolean,
             "proleakproof"    : DataType::Boolean,
             "proisstrict"     : DataType::Boolean,
             "proretset"       : DataType::Boolean,
             "provolatile"     : DataType::Utf8,
             "proparallel"     : DataType::Utf8,
             "pronargs"        : DataType::Int16,
             "pronargdefaults" : DataType::Int16,
             "prorettype"      : DataType::UInt32,
             "proargtypes"     : DataType::Utf8,
             "proallargtypes"  : DataType::Utf8 : nullable,
             "proargmodes"     : DataType::Utf8 : nullable,
             "proargnames"     : DataType::Utf8 : nullable,
             "proargdefaults"  : DataType::Utf8 : nullable,
             "protrftypes"     : DataType::Utf8 : nullable,
             "prosrc"          : DataType::Utf8,
             "probin"          : DataType::Utf8 : nullable,
             "prosqlbody"      : DataType::Utf8 : nullable,
             "proconfig"       : DataType::Utf8 : nullable,
             "proacl"          : DataType::Utf8 : nullable,
        })
    }

    pub fn pg_attribute() -> Arc<Schema> {
        schema!({
            "attrelid"       : DataType::UInt32,
            "attname"        : DataType::Utf8,
            "atttypid"       : DataType::UInt32,
            "attlen"         : DataType::Int16,
            "attnum"         : DataType::Int16,
            "attcacheoff"    : DataType::Int32,
            "atttypmod"      : DataType::Int32,
            "attndims"       : DataType::Int16,
            "attbyval"       : DataType::Boolean,
            "attalign"       : DataType::Utf8,
            "attstorage"     : DataType::Utf8,
            "attcompression" : DataType::Utf8,
            "attnotnull"     : DataType::Boolean,
            "atthasdef"      : DataType::Boolean,
            "atthasmissing"  : DataType::Boolean,
            "attidentity"    : DataType::Utf8,
            "attgenerated"   : DataType::Utf8,
            "attisdropped"   : DataType::Boolean,
            "attislocal"     : DataType::Boolean,
            "attinhcount"    : DataType::UInt16,
            "attstattarget"  : DataType::UInt16,
            "attcollation"   : DataType::UInt32,
            "attacl"         : DataType::Utf8 : nullable,
            "attoptions"     : DataType::Utf8 : nullable,
            "attfdwoptions"  : DataType::Utf8 : nullable,
            "attmissingval"  : DataType::Utf8 : nullable,
        })
    }

    pub fn pg_class() -> Arc<Schema> {
        schema! ({
             "oid"                 : DataType::UInt32,
             "relname"             : DataType::Utf8,
             "relnamespace"        : DataType::UInt32,
            //  "reltype"             : DataType::UInt32,
            //  "reloftype"           : DataType::UInt32,
            //  "relowner"            : DataType::UInt32,
            //  "relam"               : DataType::UInt32,
            //  "relfilenode"         : DataType::UInt32,
            //  "reltablespace"       : DataType::UInt32,
            //  "relpages"            : DataType::Int32,
            //  "reltuples"           : DataType::Float64,
            //  "relallvisible"       : DataType::Int32,
            //  "reltoastrelid"       : DataType::UInt32,
            //  "relhasindex"         : DataType::Boolean,
            //  "relisshared"         : DataType::Boolean,
            //  "relpersistence"      : DataType::Utf8,
             "relkind"             : DataType::Utf8,
            //  "relnatts"            : DataType::Int16,
            //  "relchecks"           : DataType::Int16,
            //  "relhasrules"         : DataType::Boolean,
            //  "relhastriggers"      : DataType::Boolean,
            //  "relhassubclass"      : DataType::Boolean,
            //  "relrowsecurity"      : DataType::Boolean,
            //  "relforcerowsecurity" : DataType::Boolean,
            //  "relispopulated"      : DataType::Boolean,
            //  "relreplident"        : DataType::Utf8,
            //  "relispartition"      : DataType::Boolean,
            //  "relrewrite"          : DataType::UInt32,
            //  "relfrozenxid"        : DataType::UInt32,
            //  "relminmxid"          : DataType::UInt32,
            //  "relacl"              : DataType::Utf8 : nullable,
            //  "reloptions"          : DataType::Utf8 : nullable,
            //  "relpartbound"        : DataType::Utf8 : nullable,
        })
    }

    pub fn pg_description() -> Arc<Schema> {
        schema!({
            "objoid"      : DataType::UInt32,
            "classoid"    : DataType::UInt32,
            "objsubid"    : DataType::Int32,
            "description" : DataType::Utf8,
        })
    }

    pub fn pg_attrdef() -> Arc<Schema> {
        schema!({
            "oid"     : DataType::UInt32,
            "adrelid" : DataType::UInt32,
            "adnum"   : DataType::Int16,
            "adbin"   : DataType::Utf8,
        })
    }
}
