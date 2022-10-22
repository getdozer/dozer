use std::collections::HashMap;

use super::proto_service::{GrpcType, ProtoService};
use anyhow::Context;
use dozer_types::models::api_endpoint::ApiEndpoint;
use handlebars::Handlebars;

pub struct ProtoGenerator<'a> {
    proto_service: ProtoService,
    handlebars: Handlebars<'a>,
    schema_name: String,
}

impl ProtoGenerator<'_> {
    pub fn new(
        schema: dozer_types::types::Schema,
        schema_name: String,
        endpoint: ApiEndpoint,
    ) -> anyhow::Result<Self> {
        let proto_service = ProtoService::new(
            schema.to_owned(),
            schema_name.to_owned(),
            endpoint.to_owned(),
        )?;
        let mut proto_generator = Self {
            handlebars: Handlebars::new(),
            proto_service,
            schema_name,
        };
        proto_generator
            .register_template()
            .context("Failed to register template")?;
        Ok(proto_generator)
    }

    fn register_template(&mut self) -> anyhow::Result<()> {
        let main_template = include_str!("template/proto.tmpl");
        self.handlebars
            .register_template_string("main", main_template)
            .context("Cannot register template")?;
        Ok(())
    }

    pub fn generate_proto(&self) -> anyhow::Result<(String, HashMap<String, GrpcType>)> {
        let meta_data = self.proto_service.get_grpc_metadata()?;
        let mut output_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(format!("proto_build/{}.proto", self.schema_name.to_owned()))
            .expect("Couldn't open file");
        let result = self.handlebars.render("main", &meta_data)?;
        self.handlebars
            .render_to_write("main", &meta_data, &mut output_file)?;
        Ok((result, meta_data.functions_with_type))
    }
}
