#!/bin/bash


kubectl apply -f ../namespace.yml
kubectl apply -f ../pvc.yml
sleep 5
kubectl apply -f ../migrate.yml
sleep 5
kubectl apply -f ../app.yml
kubectl apply -f ../app-svc.yml
sleep 10
kubectl apply -f ../api.yml
kubectl apply -f ../api-svc.yml

echo "HTTP and gRPC Endpoints respectively:" 
echo "$(minikube service dozer-api-svc -n dozer --url)"