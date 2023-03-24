#!/bin/bash

kubectl apply -f dozer-namespace.yml
kubectl apply -f dozer-pvc.yml
sleep 10
kubectl apply -f dozer-migrate.yml
sleep 10
kubectl apply -f dozer-app.yml
kubectl apply -f dozer-app-svc.yml
sleep 10
kubectl apply -f dozer-api.yml
kubectl apply -f dozer-api-svc.yml

echo "Local Endpoint: $(minikube service dozer-api-svc -n dozer --url)"