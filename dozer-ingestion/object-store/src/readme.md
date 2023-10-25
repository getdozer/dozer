## Object store connector

This connector uses local or cloud file system to ingest data, which are stored in files.
At the moment connector supports only append-only data changes. Also, current implementation only supports csv and parquet files stored locally or in s3 bucket.

Depending on storage type configuration of connection is slightly different.
Example configuration:
```yaml
connections:
  - db_type: ObjectStore
    name: data_s3
    authentication: !S3Storage
        details:
          access_key_id: {{ ACCESS_ID }}
          secret_access_key: {{ ACCESS_KEY }}
          region: ap-southeast-1
          bucket_name: {{ BUCKET }}
        tables:
          - !Table
              name: userdata
              config: !Parquet
                path: userdata_parquet
                extension: .parquet #optional

  - db_type: ObjectStore
    name: data_local
    authentication: !LocalStorage
      details:
        path: "/Users/user/data"
      tables:
        - !Table
            name: taxi_data
            config: !CSV
              path: taxi_data
              extension: .csv
```
