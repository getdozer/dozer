use bytes::Buf;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde_json::de::Deserializer;
use serde_json::Value;
use tonic::{
    codec::{Codec, Decoder, Encoder},
    Code, Status,
};
use crate::proto_util::get_proto_descriptor;

pub struct MyCodec {
    request_name: String,
    response_name: String,
    descriptor_pool: DescriptorPool,
}
impl MyCodec {
    pub fn new(request_name: String, response_name: String, descriptor_path: String) -> Self {
        let pool_construct = get_proto_descriptor(descriptor_path).unwrap();
        Self {
            request_name,
            response_name,
            descriptor_pool: pool_construct,
        }
    }
}

impl Codec for MyCodec {
    type Encode = Value;

    type Decode = DynamicMessage;

    type Encoder = MyEncoder;

    type Decoder = MyDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        return MyEncoder {
            descriptor_pool: self.descriptor_pool.to_owned(),
            message_name: self.response_name.to_owned(),
        };
    }

    fn decoder(&mut self) -> Self::Decoder {
        return MyDecoder {
            descriptor_pool: self.descriptor_pool.to_owned(),
            message_name: self.request_name.to_owned(),
        };
    }
}

pub struct MyEncoder {
    descriptor_pool: DescriptorPool,
    message_name: String,
}

impl MyEncoder {
    fn _get_message_descriptor(&self) -> Result<MessageDescriptor, Status> {
        let message_by_name = self
            .descriptor_pool
            .get_message_by_name(&self.message_name)
            .ok_or(Status::internal("Cannot ".to_owned()));
        return message_by_name;
    }
}
impl Encoder for MyEncoder {
    type Item = Value;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        println!("==== hit encode {:?}", item);
        // let message_descriptor = self._get_message_descriptor()?;
        // let mut my_dynamic = DynamicMessage::new(message_descriptor);
        // let get_field_by_name = my_dynamic.get_field_by_name("message").unwrap();
        // println!("==== field {:?}",get_field_by_name);
        // my_dynamic.set_field_by_name(
        //     "message",
        //     prost_reflect::Value::String("Hello 1233".to_owned()),
        // );
        // my_dynamic
        //     .encode(dst)
        //     .map_err(|err| Status::from_error(Box::new(err)))?;
        //     Ok(())

        let message_descriptor = self._get_message_descriptor()?;
        println!("===== message_descriptor {:?}", message_descriptor);
        let json = &item.to_string();
        println!("===== item.to_string  {:?}", json);

        let mut deserializer = Deserializer::from_str(json);

        let dynamic_message =
            DynamicMessage::deserialize(message_descriptor, &mut deserializer).unwrap();
        deserializer.end().unwrap();
        dynamic_message
            .encode(dst)
            .map_err(|err| Status::from_error(Box::new(err)))?;
        Ok(())
    }
}

pub struct MyDecoder {
    descriptor_pool: DescriptorPool,
    message_name: String,
}

impl MyDecoder {
    fn _get_message_descriptor(&self) -> Result<MessageDescriptor, Status> {
        let message_by_name = self
            .descriptor_pool
            .get_message_by_name(&self.message_name)
            .ok_or(Status::internal("Cannot ".to_owned()));
        return message_by_name;
    }
}

impl Decoder for MyDecoder {
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
        return dynamic_message;
    }
}

fn from_decode_error(error: prost::DecodeError) -> Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::new(Code::Internal, error.to_string())
}
