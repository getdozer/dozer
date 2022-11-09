use crossbeam::channel;
use dozer_types::{
    events::Event,
    serde_json::{self, json},
    types::Record,
};
use tokio::time;

use crate::{
    api_server::PipelineDetails,
    errors::GenerationError,
    generator::protoc::{generator::ProtoGenerator, proto_service::GrpcType},
    grpc::util::create_descriptor_set,
    test_utils, CacheEndpoint,
};
use std::{collections::HashMap, io, thread};

pub fn generate_proto(
    dir_path: String,
    schema_name: String,
    schema: Option<dozer_types::types::Schema>,
) -> Result<(std::string::String, HashMap<std::string::String, GrpcType>), GenerationError> {
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = vec![PipelineDetails {
        schema_name: schema_name.clone(),
        cache_endpoint: CacheEndpoint {
            cache: test_utils::initialize_cache(&schema_name, schema),
            endpoint,
        },
    }];
    let proto_generator = ProtoGenerator::new(pipeline_details)?;
    let generated_proto = proto_generator.generate_proto(dir_path)?;
    Ok(generated_proto)
}

pub fn generate_descriptor(tmp_dir: String) -> Result<String, io::Error> {
    let descriptor_path = create_descriptor_set(&tmp_dir, "generated.proto")?;
    Ok(descriptor_path)
}

pub fn mock_event_notifier() -> channel::Receiver<Event> {
    let (sender, receiver) = channel::unbounded::<Event>();
    let _executor_thread = thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(1000));
        let record_json = json!({"schema_id":{"id":1811503150,"version":1},"values":[{"Int":1048},{"String":"Test33"},"Null",{"Int":2006}]});
        let fake_record: Record = serde_json::from_value(record_json).unwrap();
        sender.try_send(Event::RecordInsert(fake_record)).unwrap();
    });
    receiver
}
