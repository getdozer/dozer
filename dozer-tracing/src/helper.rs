fn map_record_to_dozer_record(
    record: &tracing::span::Record<'_>,
) -> dozer_types::tracing::span::Record<'_> {
    let mut fields = Vec::new();
    record.record(&mut fields);
    dozer_types::tracing::span::Record { fields }
}
