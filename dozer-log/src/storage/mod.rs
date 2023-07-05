use std::{path::PathBuf, time::SystemTime};

use aws_sdk_s3::{
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError, create_bucket::CreateBucketError,
        create_multipart_upload::CreateMultipartUploadError, delete_bucket::DeleteBucketError,
        delete_objects::DeleteObjectsError, get_object::GetObjectError,
        list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
        upload_part::UploadPartError,
    },
};
use aws_smithy_types::date_time::ConversionError;
use dozer_types::{
    bytes::Bytes,
    thiserror,
    tonic::{async_trait, codegen::futures_core::Stream},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Object {
    pub key: String,
    pub last_modified: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ListObjectsOutput {
    pub objects: Vec<Object>,
    pub continuation_token: Option<String>,
}

#[async_trait]
pub trait Storage: Clone + Send + Sync + 'static {
    async fn put_object(&self, key: String, data: Vec<u8>) -> Result<(), Error>;

    /// Returns the upload id.
    async fn create_multipart_upload(&self, key: String) -> Result<String, Error>;
    /// Returns the entity tag of the part.
    async fn upload_part(
        &self,
        key: String,
        upload_id: String,
        part_number: i32,
        data: Vec<u8>,
    ) -> Result<String, Error>;
    /// Parts are (part_number, entity_tag) pairs.
    async fn complete_multipart_upload(
        &self,
        key: String,
        upload_id: String,
        parts: Vec<(i32, String)>,
    ) -> Result<(), Error>;

    async fn list_objects(
        &self,
        prefix: String,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsOutput, Error>;

    type StreamError: Into<std::io::Error> + std::fmt::Debug;
    type Stream: Stream<Item = Result<Bytes, Self::StreamError>> + Unpin;

    async fn get_object(&self, key: String) -> Result<Self::Stream, Error>;
}

mod s3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("create bucket: {0}")]
    CreateBucket(#[from] SdkError<CreateBucketError>),
    #[error("delete objects: {0}")]
    DeleteObjects(#[from] SdkError<DeleteObjectsError>),
    #[error("delete bucket: {0}")]
    DeleteBucket(#[from] SdkError<DeleteBucketError>),
    #[error("put object: {0}")]
    PutObject(#[from] SdkError<PutObjectError>),
    #[error("create multipart upload: {0}")]
    CreateMultipartUpload(#[from] SdkError<CreateMultipartUploadError>),
    #[error("upload part: {0}")]
    UploadPart(#[from] SdkError<UploadPartError>),
    #[error("complete multipart upload: {0}")]
    CompleteMultipartUpload(#[from] SdkError<CompleteMultipartUploadError>),
    #[error("list objects v2: {0}")]
    ListObjectsV2(#[from] SdkError<ListObjectsV2Error>),
    #[error("conversion: {0}")]
    Conversion(#[from] ConversionError),
    #[error("get object: {0}")]
    GetObject(#[from] SdkError<GetObjectError>),
    #[error("file system: {0}: {1}")]
    FileSystem(String, #[source] std::io::Error),
    #[error("temp dir: {0}: {1}")]
    TempDir(String, #[source] std::io::Error),
    #[error("non-utf8 path: {0:?}")]
    NonUtf8Path(PathBuf),
    #[error("upload not found: key {key}, upload id {upload_id}")]
    UploadNotFound { key: String, upload_id: String },
}

pub use s3::S3Storage;

mod local;

pub use local::LocalStorage;

#[cfg(test)]
mod tests;
