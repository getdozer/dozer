## Prerequisites

1. Local Minikube cluster
2. kubectl command line utility
3. Mount your home directory on a /data folder for minikube

```bash
minikube mount $HOME:/data
```
## Running

1. Put your dozer-config.yaml for app in dozer_data folder (you need to create dozer_data directory in root)
2. Put your dozer-config.yaml for api in ./dozer_data/api_config/ . Add the following lines to your config to configure api:
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
./dozer-data/data/trips/fhvhv_tripdata_2022-01.parquet
```
4. Run the k8-setup.sh to do an instant setup.
```
./k8-setup.sh
```
5. k8-cleanup.sh script can be run to do a complete cleanup.