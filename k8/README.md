## Prerequisites

1. Local Minikube cluster
2. kubectl command line utility
3. Mount the parent directory for your k8 folder on  /data folder for minikube

```bash
minikube mount ParentDir:/data
```
4. Execute the init.sh script

## Running

1. Put your app-config.yaml for app in workspace folder
2. Put your api-config.yaml for api in wokspace folder . Add the following lines to your config to configure api in the api-config.yaml:
```
api:
  rest:
    port: 8080
  grpc:
    port: 50051
  app_grpc:
    port: 50052
    host: app-svc.dozer
```

3. Put your data set in the dozer_data inside data folder. Example:
```bash
./workspace/data/trips/fhvhv_tripdata_2022-01.parquet
```
4. Run the k8-setup.sh to do an instant setup.
```
./k8-setup.sh
```
5. k8-cleanup.sh script can be run to do a complete cleanup.