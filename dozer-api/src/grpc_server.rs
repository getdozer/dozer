use crate::{
    api_server::PipelineDetails,
    errors::GRPCError,
    generator::protoc::generator::ProtoGenerator,
    grpc::{
        server::TonicServer,
        util::{create_descriptor_set, read_file_as_byte},
    },
};
use dozer_cache::cache::{Cache, LmdbCache};
use dozer_types::{log::debug, models::api_endpoint::ApiEndpoint};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
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
    pub fn run(
        &self,
        endpoint: ApiEndpoint,
        cache: Arc<LmdbCache>,
        running: Arc<AtomicBool>,
    ) -> Result<(), GRPCError> {
        let schema_name = endpoint.name.to_owned();
        let schema = cache.get_schema_by_name(&schema_name)?;
        let tmp_dir = TempDir::new("proto_generated").unwrap();
        let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());
        let pipeline_details = PipelineDetails {
            schema_name: schema_name.to_owned(),
            endpoint,
        };
        let proto_generator = ProtoGenerator::new(schema, pipeline_details.to_owned())?;
        let generated_proto = proto_generator.generate_proto(tempdir_path.to_owned())?;
        let descriptor_path = create_descriptor_set(
            &tempdir_path,
            &format!("{}.proto", schema_name.to_lowercase()),
        )?;
        let vec_byte = read_file_as_byte(descriptor_path.to_owned())?;
        let inflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(vec_byte.as_slice())
            .build()?;
        let addr = format!("[::1]:{:}", self.port).parse().unwrap(); // "[::1]:50051".parse().unwrap();
        let grpc_service =
            TonicServer::new(descriptor_path, generated_proto.1, cache, pipeline_details);
        let server_future = Server::builder()
            .accept_http1(true)
            .add_service(inflection_service)
            .add_service(tonic_web::enable(grpc_service))
            .serve_with_shutdown(addr, async move {
                while running.load(Ordering::SeqCst) {}
                debug!("Exiting GRPC Server on Ctrl-C");
            });
        // .serve(addr);
        let rt = Runtime::new().unwrap();
        rt.block_on(server_future)
            .expect("failed to successfully run the future on RunTime");
        Ok(())
    }
}
