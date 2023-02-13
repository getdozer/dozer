use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

use prost_reflect::DescriptorPool;

use crate::errors::GenerationError;

pub fn get_proto_descriptor(
    descriptor_path: &Path,
) -> Result<(Vec<u8>, DescriptorPool), GenerationError> {
    let descriptor_bytes = read_file_as_byte(descriptor_path)
        .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

    let descriptor = DescriptorPool::decode(descriptor_bytes.as_slice())
        .map_err(GenerationError::ProtoDescriptorError)?;

    Ok((descriptor_bytes, descriptor))
}

fn read_file_as_byte(path: &Path) -> Result<Vec<u8>, io::Error> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}
