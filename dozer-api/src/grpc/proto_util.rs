use anyhow::Ok;
use prost_reflect::DescriptorPool;
use std::{
    fs::File,
    io::{BufReader, Read},
};

//https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/DescriptorProtos.FieldDescriptorProto.Type
pub fn get_proto_descriptor(descriptor_dir: String) -> anyhow::Result<DescriptorPool> {
    let descriptor_set_dir = descriptor_dir;

    let buffer = read_file_as_byte(descriptor_set_dir)?;
    let my_array_byte = buffer.as_slice();
    let pool2 = DescriptorPool::decode(my_array_byte).unwrap();
    Ok(pool2)
}

pub fn read_file_as_byte(path: String) -> anyhow::Result<Vec<u8>> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub fn create_descriptor_set(proto_path: &str) -> anyhow::Result<String> {
    let my_path = "proto_build".to_owned();
    let my_path_descriptor = "proto_build/file_descriptor_set.bin".to_owned();
    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(my_path.to_owned());
    // Builder::new()
    //     .file_descriptor_set_path(my_path_descriptor.to_owned())
    //     .compile_protos_with_config(prost_build_config, &["proto_build/helloworld.proto"], &["proto_build/"])
    //     .expect("Failed to compile protos");

    let mut prost_build_config2 = prost_build::Config::new();
    prost_build_config2.out_dir(my_path.to_owned());
    tonic_build::configure()
        .file_descriptor_set_path(&my_path_descriptor)
        .disable_package_emission()
        .build_client(false)
        .build_server(false)
        .out_dir(&my_path)
        .compile_with_config(prost_build_config2, &[proto_path], &["proto_build/"])?;
    Ok(my_path_descriptor)
}
