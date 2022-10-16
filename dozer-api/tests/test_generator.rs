#[cfg(test)]
mod tests {
    use dozer_api::generator::oapi::generator::OpenApiGenerator;

    #[test]
    fn test_generate_oapi() -> anyhow::Result<()> {
        use openapiv3::OpenAPI;

        let schema_str = include_str!("oapi-generator/cache_film_schema.json");
        let schema: dozer_types::types::Schema = serde_json::from_str(schema_str)?;
        let endpoint_str = include_str!("oapi-generator/endpoint.json");
        let endpoint: dozer_types::models::api_endpoint::ApiEndpoint =
            serde_json::from_str(endpoint_str)?;
        let expected_str = include_str!("oapi-generator/films-expected.json");
        let expected: OpenAPI = serde_yaml::from_str(expected_str)?;
        let oapi_generator = OpenApiGenerator::new(
            schema,
            endpoint.name.to_owned(),
            endpoint,
            vec![format!("http://localhost:{}", "8080")],
        )?;
        let generated = oapi_generator
            .generate_oas3(Some("tests/oapi-generator/test_generate.json".to_owned()))?;
        assert_eq!(generated, expected, "must be equal");
        Ok(())
    }
}
