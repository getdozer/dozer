use super::proto_service::{GrpcType, ProtoService, RPCFunction, RPCMessage};
use crate::{api_server::PipelineDetails, errors::GenerationError};
use dozer_cache::cache::Cache;
use dozer_types::serde::{self, Deserialize, Serialize};
use handlebars::Handlebars;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(crate = "self::serde")]
pub struct CombineProtoMetadata {
    package_name: String,
    service_definition: Vec<ServiceDefinition>,
    import_libs: Vec<String>,
    messages: Vec<RPCMessage>,
    pub functions_with_type: HashMap<String, GrpcType>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct ServiceDefinition {
    service_name: String,
    rpc_functions: Vec<RPCFunction>,
}

pub struct ProtoGenerator<'a> {
    proto_services: Vec<ProtoService>,
    handlebars: Handlebars<'a>,
}

impl ProtoGenerator<'_> {
    pub fn new(pipeline_map: HashMap<String, PipelineDetails>) -> Result<Self, GenerationError> {
        let vec_proto_svc = pipeline_map
            .iter()
            .map(|(_, x)| {
                let cache = x.to_owned().cache_endpoint.cache;
                let endpoint = x.to_owned().cache_endpoint.endpoint;
                let schema_name = endpoint.name.to_owned();
                let schema = cache.get_schema_by_name(&schema_name).unwrap();
                ProtoService::new(schema, schema_name, endpoint)
            })
            .collect();
        let mut proto_generator = Self {
            handlebars: Handlebars::new(),
            proto_services: vec_proto_svc,
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

        let meta_datas: Vec<super::proto_service::ProtoMetadata> = self
            .proto_services
            .iter()
            .map(|svc| svc.get_grpc_metadata().unwrap())
            .collect();

        let mut combine_meta_data = CombineProtoMetadata::default();
        for metadata in meta_datas {
            combine_meta_data.package_name = metadata.package_name;

            combine_meta_data.import_libs =
                [combine_meta_data.import_libs, metadata.import_libs].concat();
            combine_meta_data.import_libs.sort();
            combine_meta_data.import_libs.dedup();

            combine_meta_data.service_definition = [
                combine_meta_data.service_definition,
                vec![ServiceDefinition {
                    service_name: metadata.service_name,
                    rpc_functions: metadata.rpc_functions,
                }],
            ]
            .concat();

            let mut messages = [combine_meta_data.messages, metadata.messages].concat();
            messages.sort_by_key(|k| format!("{} {}", k.name, k.props.join("")));
            messages.dedup_by_key(|k| format!("{} {}", k.name, k.props.join("")));
            combine_meta_data.messages = messages;

            combine_meta_data.functions_with_type = combine_meta_data
                .functions_with_type
                .into_iter()
                .chain(metadata.functions_with_type)
                .collect();
        }
        // let meta_data = self.proto_service.get_grpc_metadata()?;
        let mut output_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(format!("{}/generated.proto", folder_path,))
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        let result = self
            .handlebars
            .render("main", &combine_meta_data)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        self.handlebars
            .render_to_write("main", &combine_meta_data, &mut output_file)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        Ok((result, combine_meta_data.functions_with_type))
    }
}
