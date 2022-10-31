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
use dozer_types::{events::Event, models::api_endpoint::ApiEndpoint};
use std::{sync::Arc, thread};
use tempdir::TempDir;
use tokio::{runtime::Runtime, sync::broadcast};
use tonic::transport::Server;

pub struct GRPCServer {
    port: u16,
    event_notifier: crossbeam::channel::Receiver<Event>,
}

impl GRPCServer {
    pub fn setup_broad_cast_channel(
        sender: broadcast::Sender<Event>,
        event_notifier: crossbeam::channel::Receiver<Event>,
    ) -> Result<(), GRPCError> {
        let _thread = thread::spawn(|| {
            Runtime::new().unwrap().block_on(async {
                tokio::spawn(async move {
                    while let Some(event) = event_notifier.iter().next() {
                        _ = sender.send(event);
                    }
                });
            });
        });
        Ok(())
    }
    fn _start_grpc_server(
        &self,
        endpoint: ApiEndpoint,
        cache: Arc<LmdbCache>,
        event_notifier: crossbeam::channel::Receiver<Event>,
    ) -> Result<(), GRPCError> {
        // create broadcast channel
        let (tx, rx1) = broadcast::channel::<Event>(16);
        GRPCServer::setup_broad_cast_channel(tx, event_notifier)?;
        // let _thread = thread::spawn(|| {
        //     Runtime::new().unwrap().block_on(async {
        //         tokio::spawn(async move {
        //             while let Some(event) = event_notifier.iter().next() {
        //                 _ = tx.send(event);
        //             }
        //         });
        //     });
        // });

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
        let grpc_service = TonicServer::new(
            descriptor_path,
            generated_proto.1,
            cache,
            pipeline_details,
            rx1,
        );
        let grpc_router = Server::builder()
            .accept_http1(true)
            .concurrency_limit_per_connection(32)
            .add_service(inflection_service)
            .add_service(tonic_web::enable(grpc_service));

        let server_future = grpc_router.serve(addr);
        let rt = Runtime::new().unwrap();
        rt.block_on(server_future)
            .expect("failed to successfully run the future on RunTime");
        Ok(())
    }
}

impl GRPCServer {
    pub fn new(event_notifier: crossbeam::channel::Receiver<Event>, port: u16) -> Self {
        Self {
            port,
            event_notifier,
        }
    }
    pub fn run(&self, endpoint: ApiEndpoint, cache: Arc<LmdbCache>) -> Result<(), GRPCError> {
        let event = self.event_notifier.clone().recv();
        if let Ok(Event::SchemaChange(_)) = event {
            // Only start when ensuring Schema is existed
            self._start_grpc_server(endpoint, cache, self.event_notifier.to_owned())?;
        }
        Ok(())
    }
}
