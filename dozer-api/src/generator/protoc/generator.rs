use std::fs::File;

use super::proto_service::ProtoService;
use anyhow::Context;
use dozer_types::models::api_endpoint::ApiEndpoint;
use handlebars::Handlebars;

pub struct ProtoGenerator<'a> {
    proto_service: ProtoService,
    handlebars: Handlebars<'a>,
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

    pub fn generate_proto(&self, path: String) -> anyhow::Result<()> {
        let mut output_file = File::create(&path)?;
        let meta_data = self.proto_service.get_rpc_metadata()?;
        self.handlebars
            .render_to_write("main", &meta_data, &mut output_file)?;
        Ok(())
    }
}
