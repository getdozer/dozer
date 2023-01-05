## Releasing Dozer Containers


### ECR

## Login

Dozer Contaienr
```
aws ecr create-repository \
    --repository-name dozer \
    --region ap-southeast-1
```    


Dozer Samples
```
aws ecr create-repository \
    --repository-name dozer \
    --region ap-southeast-1
```    

Docker Login
```
 aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 708185497968.dkr.ecr.ap-southeast-1.amazonaws.com
 ```