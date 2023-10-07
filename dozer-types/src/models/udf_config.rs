use schemars::JsonSchema;

use crate::serde::{Deserialize, Serialize};

use aws_sdk_s3::{
    operation::create_bucket::CreateBucketError,
    types::{
        CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration, Delete,
        ObjectIdentifier,
    },
    Client,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct UdfConfig {
    /// name of the model function
    pub name: String,
    /// setting for what type of udf to use; Default: Onnx
    pub config: UdfType,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub enum UdfType {
    Onnx(OnnxConfig),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct OnnxConfig {
    /// path to the model file
    pub s3_storage: S3Storage,
}

pub struct S3Storage {
    client: Client,
    region: BucketLocationConstraint,
    bucket_name: String,
}


pub use aws_sdk_s3::types::BucketLocationConstraint;

impl S3Storage {
    pub async fn new(region: BucketLocationConstraint, bucket_name: String) -> Result<Self, Error> {
        let config = aws_config::from_env().load().await;
        let client = Client::new(&config);
        let create_bucket_configuration = CreateBucketConfiguration::builder()
            .location_constraint(region.clone())
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
            region,
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

            self.delete_objects(objects.into_iter().map(|object| object.key).collect())
                .await?;
        }
    }
}