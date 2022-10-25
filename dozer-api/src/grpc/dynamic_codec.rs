use super::util::get_proto_descriptor;
use crate::errors::GRPCError;
use bytes::Buf;
use dozer_types::serde_json::{de::Deserializer, Value};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use tonic::{
    codec::{Codec, Decoder, Encoder},
    Code, Status,
};

pub struct DynamicCodec {
    descriptor_pool: DescriptorPool,
    request_name: String,
    response_name: String,
}
impl DynamicCodec {
    pub fn new(request_name: String, response_name: String, descriptor_path: String) -> Self {
        let pool_construct = get_proto_descriptor(descriptor_path).unwrap();
        Self {
            request_name,
            response_name,
            descriptor_pool: pool_construct,
        }
    }
}

impl Codec for DynamicCodec {
    type Encode = Value;

    type Decode = DynamicMessage;

    type Encoder = DynamicEncoder;

    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder {
            descriptor_pool: self.descriptor_pool.to_owned(),
            message_name: self.response_name.to_owned(),
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            descriptor_pool: self.descriptor_pool.to_owned(),
            message_name: self.request_name.to_owned(),
        }
    }
}

pub struct DynamicEncoder {
    descriptor_pool: DescriptorPool,
    message_name: String,
}

impl DynamicEncoder {
    fn _get_message_descriptor(&self) -> Result<MessageDescriptor, Status> {
        self.descriptor_pool
            .get_message_by_name(&self.message_name)
            .ok_or_else(|| Status::internal("Cannot ".to_owned()))
    }
}
impl Encoder for DynamicEncoder {
    type Item = Value;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let message_descriptor = self._get_message_descriptor()?;
        let json = &item.to_string();
        let mut deserializer = Deserializer::from_str(json);

        let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)
            .map_err(GRPCError::SerizalizeError)?;
        deserializer.end().map_err(GRPCError::SerizalizeError)?;
        dynamic_message
            .encode(dst)
            .map_err(|err| Status::from_error(Box::new(err)))?;
        Ok(())
    }
}

pub struct DynamicDecoder {
    descriptor_pool: DescriptorPool,
    message_name: String,
}

impl DynamicDecoder {
    fn _get_message_descriptor(&self) -> Result<MessageDescriptor, Status> {
        self.descriptor_pool
            .get_message_by_name(&self.message_name)
            .ok_or_else(|| Status::internal("Cannot ".to_owned()))
    }
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = Status;
    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        let buf = src.chunk();
        let length = buf.len();
        let descriptor = self._get_message_descriptor()?;
        let dynamic_message = DynamicMessage::decode(descriptor, buf)
            .map(Option::Some)
            .map_err(from_decode_error);
        src.advance(length);
        dynamic_message
    }
}

fn from_decode_error(error: prost::DecodeError) -> Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::new(Code::Internal, error.to_string())
}
