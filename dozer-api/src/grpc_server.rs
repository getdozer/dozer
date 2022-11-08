use crate::{
    api_server::PipelineDetails,
    errors::GRPCError,
    generator::protoc::generator::ProtoGenerator,
    grpc::{
        server::TonicServer,
        util::{create_descriptor_set, read_file_as_byte},
    },
    CacheEndpoint,
};

use dozer_types::{events::Event, types::Schema};
use heck::ToUpperCamelCase;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
    thread,
};
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
        cache_endpoints: Vec<CacheEndpoint>,
        event_notifier: crossbeam::channel::Receiver<Event>,
        _running: Arc<AtomicBool>,
    ) -> Result<(), GRPCError> {
        // create broadcast channel
        let (tx, rx1) = broadcast::channel::<Event>(16);
        GRPCServer::setup_broad_cast_channel(tx, event_notifier)?;
        let tmp_dir = TempDir::new("proto_generated").unwrap();
        let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());
        let pipeline_details: Vec<PipelineDetails> = cache_endpoints
            .iter()
            .map(|ce| PipelineDetails {
                schema_name: ce.endpoint.name.to_owned(),
                cache_endpoint: ce.to_owned(),
            })
            .collect();
        let proto_generator = ProtoGenerator::new(pipeline_details.to_owned())?;
        let generated_proto = proto_generator.generate_proto(tempdir_path.to_owned())?;

        let descriptor_path = create_descriptor_set(&tempdir_path, "generated.proto")
            .map_err(|e| {
                println!("===== e {:?}", e);
                GRPCError::InternalError(Box::new(e))
            })?;

        let vec_byte = read_file_as_byte(descriptor_path.to_owned())
            .map_err(|e| GRPCError::InternalError(Box::new(e)))?;

        let inflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(vec_byte.as_slice())
            .build()?;
        let mut pipeline_hashmap: HashMap<String, PipelineDetails> = HashMap::new();
        for pipline_detail in pipeline_details {
            pipeline_hashmap.insert(
                format!(
                    "Dozer.{}Service",
                    pipline_detail.schema_name.to_upper_camel_case()
                ),
                pipline_detail,
            );
        }
        let grpc_service = TonicServer::new(
            descriptor_path.to_owned(),
            generated_proto.1.to_owned(),
            pipeline_hashmap,
            rx1.resubscribe(),
        );
        let grpc_router = Server::builder()
            .accept_http1(true)
            .concurrency_limit_per_connection(32)
            .add_service(inflection_service)
            .add_service(grpc_service);
        let addr = format!("[::1]:{:}", self.port).parse().unwrap();
        let grpc_router = grpc_router.serve(addr);
        // .serve_with_shutdown(addr, async move {
        //     while running.load(Ordering::SeqCst) {}
        //     debug!("Exiting GRPC Server on Ctrl-C");
        // });
        let rt = Runtime::new().unwrap();
        rt.block_on(grpc_router)
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
    pub fn run(
        &self,
        cache_endpoints: Vec<CacheEndpoint>,
        running: Arc<AtomicBool>,
    ) -> Result<(), GRPCError> {
        let mut schemas: HashMap<u32, Schema> = HashMap::new();
        // wait until all schema is initalize
        while schemas.len() < cache_endpoints.len() {
            let event = self.event_notifier.clone().recv();
            if let Ok(Event::SchemaChange(schema_change)) = event {
                let id = schema_change.get_id();
                schemas.insert(id, schema_change);
            }
        }
        self._start_grpc_server(cache_endpoints, self.event_notifier.to_owned(), running)?;
        Ok(())
    }
}
