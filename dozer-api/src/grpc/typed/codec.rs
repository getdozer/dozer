use dozer_services::tonic::{
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    Status,
};
use prost_reflect::prost::Message;
use prost_reflect::{DynamicMessage, MethodDescriptor};

use super::TypedResponse;

#[derive(Debug, Clone)]
pub struct TypedCodec(MethodDescriptor);

impl TypedCodec {
    pub fn new(method_desc: MethodDescriptor) -> Self {
        TypedCodec(method_desc)
    }
}

impl Codec for TypedCodec {
    type Encode = TypedResponse;
    type Decode = DynamicMessage;

    type Encoder = TypedCodec;
    type Decoder = TypedCodec;

    fn encoder(&mut self) -> Self::Encoder {
        self.clone()
    }

    fn decoder(&mut self) -> Self::Decoder {
        self.clone()
    }
}

impl Encoder for TypedCodec {
    type Item = TypedResponse;
    type Error = Status;

    fn encode(&mut self, request: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        request
            .message
            .encode(dst)
            .expect("insufficient space for message");
        Ok(())
    }
}

impl Decoder for TypedCodec {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let mut message = DynamicMessage::new(self.0.input());
        message
            .merge(src)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Some(message))
    }
}
