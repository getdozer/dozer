#!/bin/bash

curl --create-dirs -o workspace/data/trips/fhvhv_tripdata_2022-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet
kubectl apply -f namespace.yml
kubectl apply -f pvc.yml
sleep 10
kubectl apply -f migrate.yml
sleep 10
kubectl apply -f app.yml
kubectl apply -f app-svc.yml
sleep 10
kubectl apply -f api.yml
kubectl apply -f api-svc.yml

echo "HTTP and gRPC Endpoints respectively:" 
echo "$(minikube service dozer-api-svc -n dozer --url)"