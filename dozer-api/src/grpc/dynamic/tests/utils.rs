use crossbeam::channel;
use tokio::time;

use crate::{
    errors::GenerationError,
    generator::protoc::{generator::ProtoGenerator, proto_service::GrpcType},
    grpc::{
        dynamic::util::create_descriptor_set,
        internal_grpc::{pipeline_request::ApiEvent, PipelineRequest},
        types::{value, Operation, OperationType, Record, Value},
    },
    test_utils, CacheEndpoint, PipelineDetails,
};
use std::{collections::HashMap, io, thread};

pub fn generate_proto(
    dir_path: String,
    schema_name: String,
    schema: Option<dozer_types::types::Schema>,
) -> Result<(std::string::String, HashMap<std::string::String, GrpcType>), GenerationError> {
    let endpoint = test_utils::get_endpoint();
    let mut map = HashMap::new();
    map.insert(
        schema_name.clone(),
        PipelineDetails {
            schema_name: schema_name.clone(),
            cache_endpoint: CacheEndpoint {
                cache: test_utils::initialize_cache(&schema_name, schema),
                endpoint,
            },
        },
    );
    let proto_generator = ProtoGenerator::new(map)?;
    let generated_proto = proto_generator.generate_proto(dir_path)?;
    Ok(generated_proto)
}

pub fn generate_descriptor(tmp_dir: String) -> Result<String, io::Error> {
    let descriptor_path = create_descriptor_set(&tmp_dir, "generated.proto")?;
    Ok(descriptor_path)
}

pub fn mock_event_notifier() -> channel::Receiver<PipelineRequest> {
    let (sender, receiver) = channel::unbounded::<PipelineRequest>();
    let _executor_thread = thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(1000));

        let fake_event = PipelineRequest {
            endpoint: "users".to_string(),
            api_event: Some(ApiEvent::Op(Operation {
                typ: OperationType::Insert as i32,
                old: None,
                new: Some(Record {
                    values: vec![Value {
                        value: Some(value::Value::UintValue(32)),
                    }],
                }),
                endpoint_name: "users".to_string(),
            })),
        };
        sender.try_send(fake_event).unwrap();
    });
    receiver
}
