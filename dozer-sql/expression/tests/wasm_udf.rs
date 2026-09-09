use std::sync::Arc;

use dozer_sql_expression::{
    builder::ExpressionBuilder,
    error::Error,
    execution::Expression,
    sqlparser::{
        ast::{SelectItem, SetExpr, Statement},
        dialect::DozerDialect,
        parser::Parser,
    },
};
use dozer_types::{models::config::Config, serde_yaml, types::Schema};

fn build(sql: &str, schema: &Schema, yaml: &str) -> Result<Expression, Error> {
    let (expression, _) = plan(sql, schema, yaml, false)?;
    expression.get_type(schema)?;
    Ok(expression)
}

fn plan(
    sql: &str,
    schema: &Schema,
    yaml: &str,
    aggregates: bool,
) -> Result<(Expression, Vec<Expression>), Error> {
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let statements = Parser::parse_sql(&DozerDialect {}, sql).unwrap();
    let Statement::Query(query) = &statements[0] else {
        panic!("expected a SELECT query");
    };
    let SetExpr::Select(select) = &*query.body else {
        panic!("expected a SELECT body");
    };
    let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
        panic!("expected an expression");
    };
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );
    let mut builder = ExpressionBuilder::new(schema.fields.len(), runtime.clone());
    let expression = runtime.block_on(builder.build(aggregates, expr, schema, &config.udfs))?;
    Ok((expression, builder.aggregations))
}

#[cfg(not(feature = "wasm"))]
#[test]
fn disabled_feature_is_reported_before_opening_the_module() {
    let error = build(
        "SELECT increment(1)",
        &Schema::default(),
        "version: 1\napp_name: test\nudfs:\n  - name: increment\n    config: !Wasm\n      path: missing.wasm\n",
    )
    .unwrap_err();
    assert!(matches!(error, Error::WasmNotEnabled));
}

#[cfg(feature = "wasm")]
mod enabled {
    use super::*;
    use dozer_types::types::{Field, FieldDefinition, FieldType, Record, SourceDefinition};

    fn fixture() -> (tempfile::NamedTempFile, String) {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            wat::parse_str(
                r#"(module
                    (func (export "addOne") (param i64) (result i64)
                        local.get 0 i64.const 1 i64.add)
                    (func (export "double") (param i64) (result i64)
                        local.get 0 i64.const 2 i64.mul))"#,
            )
            .unwrap(),
        )
        .unwrap();
        let yaml = format!(
            "version: 1\napp_name: test\nudfs:\n  - name: increment\n    config: !Wasm\n      path: {}\n      function: addOne\n  - name: double\n    config: !Wasm\n      path: {}\n",
            file.path().display(), file.path().display(),
        );
        (file, yaml)
    }

    #[test]
    fn yaml_configured_exports_work_in_nested_sql_and_on_nullable_rows() {
        let (_module, yaml) = fixture();
        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "value".into(),
                    FieldType::Int,
                    true,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .to_owned();
        let mut expression =
            build("SELECT double(increment(value)) FROM input", &schema, &yaml).unwrap();
        let expression_type = expression.get_type(&schema).unwrap();
        assert_eq!(expression_type.return_type, FieldType::Int);
        assert!(expression_type.nullable);
        for (input, expected) in [
            (Field::Int(10), Field::Int(22)),
            (Field::Int(-2), Field::Int(-2)),
            (Field::Null, Field::Null),
        ] {
            assert_eq!(
                expression
                    .evaluate(&Record::new(vec![input]), &schema)
                    .unwrap(),
                expected
            );
        }
        let mut arithmetic = build("SELECT increment(40 + 1)", &schema, &yaml).unwrap();
        assert_eq!(
            arithmetic.evaluate(&Record::new(vec![]), &schema).unwrap(),
            Field::Int(42)
        );
    }

    #[test]
    fn sql_planning_validates_types_arity_and_exports() {
        let (_module, yaml) = fixture();
        for sql in [
            "SELECT increment('wrong')",
            "SELECT increment()",
            "SELECT increment(1, 2)",
        ] {
            assert!(build(sql, &Schema::default(), &yaml).is_err(), "{sql}");
        }
        let invalid = yaml.replace("function: addOne", "function: missing");
        assert!(build("SELECT increment(1)", &Schema::default(), &invalid).is_err());
    }

    #[test]
    fn aggregate_arguments_are_typed_against_the_post_aggregation_schema() {
        let (_module, yaml) = fixture();
        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "value".into(),
                    FieldType::Int,
                    true,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .to_owned();
        let (mut expression, aggregates) = plan(
            "SELECT double(increment(SUM(value))) FROM input",
            &schema,
            &yaml,
            true,
        )
        .unwrap();
        assert_eq!(aggregates.len(), 1);
        let mut output_schema = schema.clone();
        for aggregate in &aggregates {
            let ty = aggregate.get_type(&schema).unwrap();
            output_schema.field(
                FieldDefinition::new(
                    aggregate.to_string(&schema),
                    ty.return_type,
                    ty.nullable,
                    ty.source,
                ),
                false,
            );
        }
        assert_eq!(
            expression.get_type(&output_schema).unwrap().return_type,
            FieldType::Int
        );
        assert_eq!(
            expression
                .evaluate(
                    &Record::new(vec![Field::Int(5), Field::Int(12)]),
                    &output_schema
                )
                .unwrap(),
            Field::Int(26)
        );
    }

    #[test]
    fn planned_expression_keeps_the_compiled_module_and_clone_semantics() {
        let (module, yaml) = fixture();
        let schema = Schema::default();
        let expression = build("SELECT increment(41)", &schema, &yaml).unwrap();
        assert_eq!(
            expression,
            build("SELECT increment(41)", &schema, &yaml).unwrap()
        );
        let mut clone = expression.clone();
        assert_eq!(expression, clone);
        drop(module);
        assert_eq!(
            clone.evaluate(&Record::new(vec![]), &schema).unwrap(),
            Field::Int(42)
        );
        assert_eq!(clone.to_string(&schema), "increment(41)");
    }

    #[test]
    fn missing_and_invalid_module_files_fail_at_planning_time() {
        let (module, yaml) = fixture();
        std::fs::write(module.path(), b"not a wasm module").unwrap();
        assert!(build("SELECT increment(1)", &Schema::default(), &yaml).is_err());
        drop(module);
        assert!(build("SELECT increment(1)", &Schema::default(), &yaml).is_err());
    }
}
