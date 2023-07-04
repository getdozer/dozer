use aws_sdk_s3::{
    operation::create_bucket::CreateBucketError,
    primitives::ByteStreamError,
    types::{
        BucketLocationConstraint, CompletedMultipartUpload, CompletedPart,
        CreateBucketConfiguration, Delete, ObjectIdentifier,
    },
    Client,
};
use aws_smithy_http::{byte_stream::ByteStream, result::SdkError};
use dozer_types::tonic::async_trait;

use super::{Error, ListObjectsOutput, Object, Storage};

#[derive(Debug, Clone)]
pub struct S3Storage {
    client: Client,
    bucket_name: String,
}

impl S3Storage {
    pub async fn new(bucket_name: String) -> Result<Self, Error> {
        let config = aws_config::from_env().load().await;
        let client = Client::new(&config);
        let create_bucket_configuration = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::UsEast2)
            .build();
        if let Err(e) = client
            .create_bucket()
            .bucket(&bucket_name)
            .create_bucket_configuration(create_bucket_configuration)
            .send()
            .await
        {
            if !is_bucket_already_owned_by_you(&e) {
                return Err(e.into());
            }
        }
        Ok(Self {
            client,
            bucket_name,
        })
    }

    pub async fn delete(self) -> Result<(), Error> {
        loop {
            let objects = self.list_objects(String::new(), None).await?.objects;
            if objects.is_empty() {
                return self
                    .client
                    .delete_bucket()
                    .bucket(&self.bucket_name)
                    .send()
                    .await
                    .map_err(Into::into)
                    .map(|_| ());
            }

            let mut delete = Delete::builder();
            for object in objects {
                delete = delete.objects(ObjectIdentifier::builder().key(object.key).build());
            }
            self.client
                .delete_objects()
                .bucket(&self.bucket_name)
                .delete(delete.build())
                .send()
                .await?;
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn put_object(&self, key: String, data: Vec<u8>) -> Result<(), Error> {
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(data.into())
            .send()
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn create_multipart_upload(&self, key: String) -> Result<String, Error> {
        self.client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map(|output| output.upload_id.expect("must have upload id"))
            .map_err(Into::into)
    }

    async fn upload_part(
        &self,
        key: String,
        upload_id: String,
        part_number: i32,
        data: Vec<u8>,
    ) -> Result<String, Error> {
        self.client
            .upload_part()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(data.into())
            .send()
            .await
            .map(|output| output.e_tag.expect("must have e tag"))
            .map_err(Into::into)
    }

    async fn complete_multipart_upload(
        &self,
        key: String,
        upload_id: String,
        parts: Vec<(i32, String)>,
    ) -> Result<(), Error> {
        let mut completed_multipart_upload = CompletedMultipartUpload::builder();
        for (part_number, e_tag) in parts.into_iter() {
            completed_multipart_upload = completed_multipart_upload.parts(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build(),
            );
        }
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_multipart_upload.build())
            .send()
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn list_objects(
        &self,
        prefix: String,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsOutput, Error> {
        let mut list_objects_builder = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(prefix);
        if let Some(continuation_token) = continuation_token {
            list_objects_builder = list_objects_builder.continuation_token(continuation_token);
        }
        let list_objects_output = list_objects_builder.send().await?;
        let objects = list_objects_output
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|object| {
                (*object.last_modified().expect("must have last modified"))
                    .try_into()
                    .map(|last_modified| Object {
                        key: object.key.expect("must have key"),
                        last_modified,
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(ListObjectsOutput {
            objects,
            continuation_token: list_objects_output.next_continuation_token,
        })
    }

    type StreamError = ByteStreamError;
    type Stream = ByteStream;

    async fn get_object(&self, key: String) -> Result<Self::Stream, Error> {
        self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map(|output| output.body)
            .map_err(Into::into)
    }
}

fn is_bucket_already_owned_by_you(error: &SdkError<CreateBucketError>) -> bool {
    if let SdkError::ServiceError(error) = error {
        error.err().is_bucket_already_owned_by_you()
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_storage_basic, test_storage_multipart, test_storage_prefix};

    use super::*;

    #[tokio::test]
    async fn test_s3_storage_basic() {
        let storage = S3Storage::new("test-s3-storage-basic-us-east-2".to_string())
            .await
            .unwrap();
        test_storage_basic(&storage).await;
        storage.delete().await.unwrap();
    }

    #[tokio::test]
    async fn test_s3_storage_multipart() {
        let storage = S3Storage::new("test-s3-storage-multipart-us-east-2".to_string())
            .await
            .unwrap();
        test_storage_multipart(&storage).await;
        storage.delete().await.unwrap();
    }

    #[tokio::test]
    async fn test_s3_storage_prefix() {
        let storage = S3Storage::new("test-s3-storage-prefix-us-east-2".to_string())
            .await
            .unwrap();
        test_storage_prefix(&storage).await;
        storage.delete().await.unwrap();
    }
}
