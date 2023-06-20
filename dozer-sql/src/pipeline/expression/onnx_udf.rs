const MODULE_NAME: &str = "onnx_udf";

pub fn evaluate_onnx_udf(
    schema: &Schema,
    name: &str,
    args: &[Expression],
    return_type: &FieldType,
    record: &Record,
) -> Result<Field, PipelineError> {
        unimplmented!()
}
