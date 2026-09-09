//! Evaluate the AssemblyScript example through Dozer's SQL parser and expression planner.
use std::{error::Error, sync::Arc};

use dozer_sql_expression::{
    builder::ExpressionBuilder,
    sqlparser::{
        ast::{SelectItem, SetExpr, Statement},
        dialect::DozerDialect,
        parser::Parser,
    },
};
use dozer_types::{
    models::config::Config,
    serde_yaml,
    types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
};

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or("usage: wasm_udf <config.yaml>")?;
    let config: Config = serde_yaml::from_str(&std::fs::read_to_string(config_path)?)?;
    let sql = config.sql.as_deref().ok_or("config must include sql")?;
    let statements = Parser::parse_sql(&DozerDialect {}, sql)?;
    let Statement::Query(query) = &statements[0] else {
        return Err("expected SELECT".into());
    };
    let SetExpr::Select(select) = &*query.body else {
        return Err("expected SELECT body".into());
    };
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
        .field(
            FieldDefinition::new(
                "amount".into(),
                FieldType::Float,
                true,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();
    let runtime = Arc::new(tokio::runtime::Builder::new_current_thread().build()?);
    let mut builder = ExpressionBuilder::new(schema.fields.len(), runtime.clone());
    println!("SQL: {}", sql.trim());
    let mut expressions = Vec::new();
    for item in &select.projection {
        let SelectItem::UnnamedExpr(expr) = item else {
            return Err("example expects expressions without aliases".into());
        };
        let expression = runtime.block_on(builder.build(false, expr, &schema, &config.udfs))?;
        expression.get_type(&schema)?;
        expressions.push(expression);
    }
    for input in [Field::Int(41), Field::Null, Field::Int(-1)] {
        let amount = match &input {
            Field::Int(value) => Field::Float((*value as f64).into()),
            _ => Field::Null,
        };
        println!(
            "\nInput value: {}, amount: {}",
            display(&input),
            display(&amount)
        );
        let record = Record::new(vec![input, amount]);
        for expression in &mut expressions {
            let label = expression.to_string(&schema);
            match expression.evaluate(&record, &schema) {
                Ok(value) => println!("  {label} = {}", display(&value)),
                Err(error) => println!("  {label}: {error}"),
            }
        }
    }
    Ok(())
}

fn display(value: &Field) -> String {
    if matches!(value, Field::Null) {
        "NULL".into()
    } else {
        value.to_string()
    }
}
