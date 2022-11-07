use super::proto_service::{GrpcType, ProtoService};
use crate::{api_server::PipelineDetails, errors::GenerationError};
use handlebars::Handlebars;
use std::collections::HashMap;
use std::path::Path;

pub struct ProtoGenerator<'a> {
    proto_service: ProtoService,
    handlebars: Handlebars<'a>,
    schema_name: String,
}

impl ProtoGenerator<'_> {
    pub fn new(
        schema: dozer_types::types::Schema,
        pipeline_details: PipelineDetails,
    ) -> Result<Self, GenerationError> {
        let proto_service = ProtoService::new(
            schema,
            pipeline_details.schema_name.to_owned(),
            pipeline_details.cache_endpoint.endpoint,
        );
        let mut proto_generator = Self {
            handlebars: Handlebars::new(),
            proto_service,
            schema_name: pipeline_details.schema_name,
        };
        proto_generator.register_template()?;
        Ok(proto_generator)
    }

    fn register_template(&mut self) -> Result<(), GenerationError> {
        let main_template = include_str!("template/proto.tmpl");
        self.handlebars
            .register_template_string("main", main_template)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        Ok(())
    }

    pub fn generate_proto(
        &self,
        folder_path: String,
    ) -> Result<(String, HashMap<String, GrpcType>), GenerationError> {
        if !Path::new(&folder_path).exists() {
            return Err(GenerationError::DirPathNotExist);
        }
        let meta_data = self.proto_service.get_grpc_metadata()?;
        let mut output_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(format!(
                "{}/{}.proto",
                folder_path,
                self.schema_name.to_owned()
            ))
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        let result = self
            .handlebars
            .render("main", &meta_data)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        self.handlebars
            .render_to_write("main", &meta_data, &mut output_file)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        Ok((result, meta_data.functions_with_type))
    }
}
