use crate::pipeline::expression::onnx_udf::evaluate_onnx_udf;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::ort::{Environment, GraphOptimizationLevel, LoggingLevel, SessionBuilder};
use dozer_types::types::{Field, FieldType, Record, Schema};
use std::path::Path;

#[test]
fn test_standard() {
    let environment = Environment::builder()
        .with_name("a1f19e3a24bd49219a0f0060200a258b")
        .with_log_level(LoggingLevel::Verbose)
        .build()
        .unwrap()
        .into_arc();

    let session = SessionBuilder::new(&environment)
        .unwrap()
        .with_optimization_level(GraphOptimizationLevel::Level1)
        .unwrap()
        .with_intra_threads(1)
        .unwrap()
        .with_model_from_file(Path::new("./models/vectorizer.onnx"))
        .expect("Could not read model from memory");

    let record = Record::new(vec![Field::String("document".to_string())]);

    let res = evaluate_onnx_udf(
        &Schema::default(),
        &session,
        &vec![],
        &FieldType::String,
        &record,
    );
    let record = res.unwrap();
}
