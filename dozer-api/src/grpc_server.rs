use crate::{
    generator::protoc::generator::ProtoGenerator,
    grpc::{
        server::TonicServer,
        util::{create_descriptor_set, read_file_as_byte},
    },
};
use dozer_cache::cache::{Cache, LmdbCache};
use dozer_types::models::api_endpoint::ApiEndpoint;
use std::sync::Arc;
use tempdir::TempDir;
use tokio::runtime::Runtime;
use tonic::transport::Server;

pub struct GRPCServer {
    port: u16,
}

impl GRPCServer {
    pub fn default() -> Self {
        Self { port: 50051 }
    }
    pub fn run(&self, endpoints: Vec<ApiEndpoint>, cache: Arc<LmdbCache>, schema_name: String) {
        let schema = cache.get_schema_by_name(&schema_name).unwrap();
        let tmp_dir = TempDir::new("proto_generated").unwrap();
        let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());

        let proto_generator =
            ProtoGenerator::new(schema, schema_name.to_owned(), endpoints[0].clone()).unwrap();
        let generated_proto = proto_generator
            .generate_proto(tempdir_path.to_owned())
            .unwrap();

        let descriptor_path = create_descriptor_set(
            &tempdir_path,
            &format!("{}.proto", schema_name.to_lowercase()),
        )
        .unwrap();
        let vec_byte = read_file_as_byte(descriptor_path.to_owned()).unwrap();
        let inflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(vec_byte.as_slice())
            .build()
            .unwrap();
        let addr = format!("[::1]:{:}", self.port).parse().unwrap(); // "[::1]:50051".parse().unwrap();
        let grpc_service = TonicServer::new(descriptor_path, generated_proto.1, cache, schema_name);
        let server_future = Server::builder()
            .add_service(inflection_service)
            .add_service(grpc_service)
            .serve(addr);
        let rt = Runtime::new().unwrap();
        rt.block_on(server_future)
            .expect("failed to successfully run the future on RunTime");
    }
}
